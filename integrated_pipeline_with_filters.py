# integrated_pipeline_with_filters.py - 带筛选系统的智能岗位处理流水线（修复版）
"""
🎯 分层筛选智能岗位处理流水线 - 修复版

核心功能：
1. 📊 分层筛选系统：
   - 步骤2后：基础筛选（硬性条件过滤） 
   - 步骤3后：高级筛选（智能评分排序）
2. 优化Notion字段结构（14个核心字段）
3. 增强用户体验和筛选反馈
4. 🔧 修复: 正确处理"无新数据"情况，避免误导性的"失败"状态

筛选特点：
- 🚫 硬性筛选：自动过滤毕业时间不符、招聘已截止的岗位
- 📈 智能评分：0-100分综合评分，基于经验匹配、薪资、公司等级等
- ⭐ 推荐等级：🌟 强烈推荐、✅ 推荐、⚠️ 一般、❌ 不推荐
- 💎 字段优化：从24个字段精简到14个核心字段

使用方式：
1. 完整流水线：python integrated_pipeline_with_filters.py
2. 调试模式：python integrated_pipeline_with_filters.py --skip-crawl --log-level debug
3. 禁用筛选：python integrated_pipeline_with_filters.py --no-filters

修复内容：
- ✅ 修复了 step2_deduplicate_and_filter_jobs 方法的返回逻辑
- ✅ 添加了无数据检查到 step3_extract_and_advanced_filter 方法
- ✅ 添加了无数据检查到 step4_write_to_notion_optimized 方法  
- ✅ 改进了主流程中的最终消息显示
- ✅ 区分有数据和无数据的成功情况
"""
import asyncio
import json
import os
import glob
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

# 导入原有组件
from src.logger_config import LogLevel, init_logger, get_logger, cleanup_logger
from src.data_snapshot import create_snapshot_manager
from src.enhanced_pipeline_fixed import EnhancedNotionJobPipelineWithLogging

# 导入统一筛选系统
from src.unified_filter_system import (
    UnifiedJobFilterManager, 
    get_unified_filter_config,
    create_optimized_notion_properties,
    get_optimized_notion_fields
)

try:
    from dotenv import load_dotenv
    # 加载环境变量
    for env_path in [".env", "../.env", "../../.env"]:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break
    
    from src.crawler_registry import crawler_registry
    from src.enhanced_extractor import EnhancedNotionExtractor
    from src.optimized_notion_writer import OptimizedNotionJobWriter
    DEPENDENCIES_OK = True
except ImportError as e:
    print(f"❌ 依赖导入失败: {e}")
    DEPENDENCIES_OK = False

class FilteredJobPipeline(EnhancedNotionJobPipelineWithLogging):
    """集成分层筛选系统的增强版流水线 - 修复版"""
    
    def __init__(self, config=None, skip_crawl=False, data_file=None, 
                 skip_notion_load=False, notion_cache_file=None, enable_filters=True):
        """初始化带筛选的流水线"""
        super().__init__(config, skip_crawl, data_file, skip_notion_load, notion_cache_file)
        
        self.enable_filters = enable_filters
        self.filter_manager = None
        
        # 筛选统计
        self.filter_stats = {
            "input_jobs": 0,
            "basic_filtered": 0,
            "advanced_filtered": 0,
            "hard_rejected": 0,
            "soft_rejected": 0,
            "final_passed": 0,
            "recommended_jobs": 0,
            "not_recommended": 0
        }
        
        if enable_filters:
            self._init_filter_system()
        
        self.logger.debug("带筛选的流水线初始化完成", {
            "enable_filters": enable_filters,
            "filter_manager_loaded": self.filter_manager is not None
        })
    
    def _init_filter_system(self):
        """初始化筛选系统"""
        try:
            # 加载筛选配置
            filter_config = get_unified_filter_config()
            
            # 可以从环境变量覆盖用户配置
            if os.getenv("USER_GRADUATION"):
                filter_config["basic"]["common"]["user_graduation"] = os.getenv("USER_GRADUATION")
            if os.getenv("USER_EXPERIENCE_YEARS"):
                filter_config["basic"]["common"]["user_experience_years"] = float(os.getenv("USER_EXPERIENCE_YEARS"))
            if os.getenv("TARGET_SALARY"):
                filter_config["basic"]["salary"]["target_salary"] = int(os.getenv("TARGET_SALARY"))
            
            self.filter_manager = UnifiedJobFilterManager(filter_config)
            
            self.logger.success("筛选系统初始化成功", {
                "user_graduation": filter_config["basic"]["common"]["user_graduation"],
                "user_experience": filter_config["basic"]["common"]["user_experience_years"],
                "target_salary": filter_config["basic"]["salary"]["target_salary"],
                "basic_filters": len(self.filter_manager.basic_filters),
                "advanced_filters": len(self.filter_manager.advanced_filters)
            })
            
        except Exception as e:
            self.logger.error("筛选系统初始化失败", {"error": str(e)}, e)
            self.filter_manager = None

    async def _post_deduplication_processing(self, deduplicated_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """去重后处理：添加基础筛选逻辑"""
        
        # 如果筛选被禁用，直接返回
        if not self.enable_filters or not self.filter_manager:
            return deduplicated_jobs
        
        self.logger.info("开始基础筛选处理", {
            "input_count": len(deduplicated_jobs),
            "filters_enabled": list(self.filter_manager.basic_filters.keys())
        })
        
        # 保存基础筛选前快照
        self.snapshot.capture("before_basic_filter", deduplicated_jobs, {
            "stage": "基础筛选前",
            "job_count": len(deduplicated_jobs)
        })
        
        # 执行基础筛选
        filtered_results = self.filter_manager.apply_basic_filters(deduplicated_jobs)
        
        input_count = len(deduplicated_jobs)
        passed_count = len(filtered_results)
        rejected_count = input_count - passed_count
        
        # 更新筛选统计
        self.filter_stats.update({
            "input_jobs": input_count,
            "basic_filtered": passed_count,
            "hard_rejected": rejected_count
        })
        
        # 保存基础筛选后快照
        self.snapshot.capture("after_basic_filter", filtered_results, {
            "stage": "基础筛选后",
            "passed_count": passed_count,
            "hard_rejected_count": rejected_count
        })
        
        if passed_count == 0:
            self.logger.info_no_data("基础筛选完成 - 无岗位通过筛选")
        else:
            self.logger.success(f"基础筛选完成 - {passed_count}个岗位通过")
        
        return filtered_results
    
    def _generate_dedup_stats(self, deduplicated_jobs: List[Dict], final_jobs: List[Dict]) -> Dict[str, Any]:
        """生成包含筛选信息的统计"""
        base_stats = super()._generate_dedup_stats(deduplicated_jobs, final_jobs)
        
        # 添加筛选统计
        if self.enable_filters:
            base_stats.update({
                "基础筛选通过": self.filter_stats.get("basic_filtered", len(final_jobs)),
                "硬性被拒": self.filter_stats.get("hard_rejected", 0),
                "通过率": f"{(len(final_jobs)/len(self.raw_jobs)*100):.1f}%" if self.raw_jobs else "0%"
            })
        
        return base_stats
    
    def get_total_steps(self) -> int:
        """子类有5个步骤"""
        return 5

    async def step3_extract_and_advanced_filter(self) -> bool:
        """步骤3: 信息提取 + 高级筛选（修复版）"""
        self.logger.step_start("信息提取 + 高级筛选", 3, 5)
        
        # 🔧 修复: 添加无数据检查
        if not self.deduplicated_jobs:
            self.logger.info("没有新岗位需要提取，跳过信息提取步骤")
            self.logger.step_end("信息提取 + 高级筛选", True, {
                "状态": "跳过(无新数据)",
                "原因": "所有岗位已存在于数据库或被筛选过滤",
                "建议": "重新爬取或使用不同搜索条件获取新岗位",
                "数据质量": "✅ 避免重复处理"
            })
            # 确保统计数据正确
            self.extracted_jobs = []
            self.stats["extracted"] = 0
            self.stats["failed"] = 0
            return True  # 🔧 返回True而不是False
        
        try:
            # 第一阶段：信息提取（原有逻辑）
            extraction_success = await self._extract_job_info()
            if not extraction_success:
                return False
            
            # 第二阶段：高级筛选和优化字段
            if self.enable_filters and self.filter_manager and self.extracted_jobs:
                self.logger.info("开始高级筛选处理", {
                    "input_count": len(self.extracted_jobs),
                    "filters_enabled": list(self.filter_manager.advanced_filters.keys())
                })
                
                # 保存高级筛选前快照
                self.snapshot.capture("before_advanced_filter", self.extracted_jobs, {
                    "stage": "高级筛选前",
                    "job_count": len(self.extracted_jobs)
                })
                
                # 执行高级筛选（智能评分）
                advanced_results = self.filter_manager.apply_advanced_filters(self.extracted_jobs)

                input_count = len(self.extracted_jobs)
                scored_count = len(advanced_results)
                
                # 更新统计
                self.filter_stats["advanced_filtered"] = scored_count
                self.filter_stats["soft_rejected"] = 0
                
                # 统计推荐等级
                for job in advanced_results:
                    if "推荐" in job.get("推荐等级", ""):
                        self.filter_stats["recommended_jobs"] += 1
                    else:
                        self.filter_stats["not_recommended"] += 1
                
                # 优化字段结构（14个核心字段）
                self.extracted_jobs = self._optimize_job_fields(advanced_results)
                
                # 保存高级筛选后快照
                self.snapshot.capture("after_advanced_filter", self.extracted_jobs, {
                    "stage": "高级筛选后",
                    "scored_count": scored_count,
                    "soft_rejected_count": 0,
                    "recommended_count": self.filter_stats["recommended_jobs"]
                })
                
                step_stats = {
                    "提取成功": len(self.extracted_jobs),
                    "推荐岗位": self.filter_stats["recommended_jobs"],
                    "一般岗位": self.filter_stats["not_recommended"],
                    "软拒绝": 0,
                    "推荐率": f"{(self.filter_stats['recommended_jobs']/len(self.extracted_jobs)*100):.1f}%" if self.extracted_jobs else "0%"
                }
                
                self.logger.success("高级筛选完成", step_stats)
                self.logger.step_end("信息提取 + 高级筛选", True, step_stats)
                
            else:
                self.logger.info("筛选功能已禁用，跳过高级筛选")
                # 仍然需要优化字段
                self.extracted_jobs = self._optimize_job_fields(self.extracted_jobs)
                self.logger.step_end("信息提取 + 高级筛选", True, {
                    "提取成功": len(self.extracted_jobs),
                    "字段优化": "完成（14个核心字段）"
                })
            
            return True
            
        except Exception as e:
            self.logger.error("信息提取+高级筛选失败", {"error": str(e)}, e)
            self.logger.step_end("信息提取 + 高级筛选", False, {"错误": str(e)})
            return False
    
    async def step4_write_to_notion_optimized(self) -> bool:
        """步骤4: 写入优化的Notion数据库（修复版）"""
        self.logger.step_start("写入Notion数据库", 4, 5)
        
        # 🔧 修复: 添加无数据检查
        if not self.extracted_jobs:
            self.logger.info("没有新岗位需要写入，跳过写入步骤")
            self.logger.step_end("写入Notion数据库", True, {
                "状态": "跳过(无新数据)",
                "原因": "没有通过筛选的新岗位",
                "数据库状态": "✅ 保持最新",
                "建议": "调整筛选条件或获取新数据"
            })
            return True  # 🔧 返回True而不是False
        
        try:
            # 批量写入优化的岗位数据
            write_stats = await self._batch_write_optimized_jobs(self.extracted_jobs)
            
            # 更新最终统计
            self.filter_stats["final_passed"] = write_stats["success"]
            
            step_stats = {
                "写入成功": write_stats["success"],
                "写入失败": write_stats["failed"],
                "推荐岗位": write_stats.get("strongly_recommended", 0) + write_stats.get("recommended", 0),
                "一般岗位": write_stats.get("considerable", 0) + write_stats.get("not_recommended", 0),
                "成功率": f"{(write_stats['success']/write_stats['total']*100):.1f}%" if write_stats['total'] > 0 else "0%"
            }
            
            if write_stats["success"] > 0:
                self.logger.success("Notion写入完成", step_stats)
            else:
                self.logger.warning("没有岗位成功写入Notion", step_stats)
            
            self.logger.step_end("写入Notion数据库", True, step_stats)
            return True
            
        except Exception as e:
            self.logger.error("Notion写入失败", {"error": str(e)}, e)
            self.logger.step_end("写入Notion数据库", False, {"错误": str(e)})
            return False
    
    async def step5_generate_final_report(self) -> bool:
        """步骤5: 生成最终报告"""
        self.logger.step_start("生成最终报告", 5, 5)
        
        try:
            # 保存快照摘要
            self.snapshot.save_summary()
            
            self.logger.step_end("生成最终报告", True, {
                "快照数量": len(self.snapshot.snapshots),
                "报告状态": "已生成"
            })
            
            return True
            
        except Exception as e:
            self.logger.error("生成最终报告失败", {"error": str(e)}, e)
            self.logger.step_end("生成最终报告", False, {"错误": str(e)})
            # 即使失败也不影响整体流水线
            return True

    async def run_filtered_pipeline(self) -> bool:
        """运行带筛选的完整流水线（修复版）"""
        self.logger.info("🚀 启动带筛选的智能岗位处理流水线（修复版）")
        
        if self.enable_filters:
            if self.filter_manager:
                self.logger.info("✅ 筛选系统已启用")
            else:
                self.logger.warning("⚠️ 筛选系统初始化失败，将使用原始流水线")
                self.enable_filters = False
        else:
            self.logger.info("ℹ️ 筛选系统已禁用（测试模式）")
        
        try:
            # 步骤1: 爬取或加载数据
            if not await self.step1_load_or_crawl_jobs():
                return False
            
            # 步骤2: 去重 + 基础筛选
            if not await self.step2_deduplicate_jobs():
                return False
            
            # 步骤3: 信息提取 + 高级筛选
            if not await self.step3_extract_and_advanced_filter():
                return False
            
            # 步骤4: 写入优化的Notion数据库
            if not await self.step4_write_to_notion_optimized():
                return False
            
            # 步骤5: 生成最终报告
            await self.step5_generate_final_report()
            
            # 🔧 修复: 区分不同的成功情况
            if self.extracted_jobs and len(self.extracted_jobs) > 0:
                self.logger.success("🎉 带筛选的流水线执行完成", self._get_final_stats())
                self._show_usage_suggestions()
            else:
                self.logger.success_no_data("✅ 流水线执行成功 - 数据库已是最新状态", self._get_final_stats())
                self.logger.info("ℹ️ 这是正常情况，说明去重和筛选系统工作正常")
                self._show_optimization_suggestions()
            
            return True
            
        except Exception as e:
            self.logger.error("流水线执行失败", {"error": str(e)}, e)
            return False
    
    def _get_final_stats(self) -> Dict[str, Any]:
        """获取最终统计信息"""
        return {
            "总爬取岗位": len(self.raw_jobs) if self.raw_jobs else 0,
            "去重后岗位": self.filter_stats.get("input_jobs", 0),
            "基础筛选通过": self.filter_stats.get("basic_filtered", 0),
            "信息提取成功": len(self.extracted_jobs) if self.extracted_jobs else 0,
            "推荐岗位数": self.filter_stats.get("recommended_jobs", 0),
            "最终写入": self.filter_stats.get("final_passed", 0),
            "整体通过率": f"{(self.filter_stats.get('final_passed', 0) / max(len(self.raw_jobs) if self.raw_jobs else 1, 1) * 100):.1f}%"
        }
    
    def _show_usage_suggestions(self):
        """显示使用建议"""
        self.logger.info("📱 Notion使用建议:")
        self.logger.info("   1. 按\"综合评分\"降序排列查看最优岗位")
        self.logger.info("   2. 筛选\"推荐等级\" = \"🌟 强烈推荐\"查看顶级岗位")
        self.logger.info("   3. 查看\"经验匹配建议\"了解申请策略")
        self.logger.info("   4. 关注\"招聘截止日期_标准化\"合理安排时间")
    
    def _show_optimization_suggestions(self):
        """显示优化建议"""
        self.logger.info("💡 优化建议:")
        self.logger.info("   1. 尝试不同的搜索关键词")
        self.logger.info("   2. 调整筛选条件（USER_GRADUATION, TARGET_SALARY等）")
        self.logger.info("   3. 扩大搜索地区范围")
        self.logger.info("   4. 检查是否需要更新简历匹配条件")
    
    async def _extract_job_info(self) -> bool:
        """信息提取处理"""
        if not self.deduplicated_jobs:
            self.logger.warning("没有去重后的岗位数据")
            return False
        
        try:
            self.logger.info(f"开始提取 {len(self.deduplicated_jobs)} 个岗位的详细信息")
            
            extractor = EnhancedNotionExtractor()
            extracted_jobs = []
            failed_count = 0
            
            for i, job in enumerate(self.deduplicated_jobs, 1):
                try:
                    self.logger.debug(f"提取第 {i}/{len(self.deduplicated_jobs)} 个岗位", {
                        "job_title": job.get("岗位名称", "N/A"),
                        "company": job.get("公司名称", "N/A")
                    })

                    # ✅ 提取必需的参数
                    html = job.get('html', '')
                    url = job.get('岗位链接', '')
                
                    if not html:
                        self.logger.warning(f"岗位 {i} 没有HTML内容", {
                            "job_title": job.get('岗位名称', 'N/A'),
                            "company": job.get('公司名称', 'N/A')
                        })
                        failed_count += 1
                        continue
                
                    # ✅ 使用正确的方法调用
                    extracted_job = await extractor.extract_for_notion_enhanced(html, url, job)
                    
                    if extracted_job:
                        extracted_jobs.append(extracted_job)
                        self.logger.debug(f"✅ 第 {i} 个岗位提取成功")
                    else:
                        failed_count += 1
                        self.logger.warning(f"❌ 第 {i} 个岗位提取失败")
                        
                except Exception as e:
                    self.logger.warning(f"第 {i} 个岗位提取异常", {
                        "job_title": job.get("岗位名称", "N/A"),
                        "error": str(e)
                    }, e)
                    failed_count += 1
                
                # API调用间隔
                await asyncio.sleep(1.5)
            
            self.extracted_jobs = extracted_jobs
            self.stats["extracted"] = len(extracted_jobs)
            self.stats["failed"] = failed_count
            
            # 保存提取结果快照
            if extracted_jobs:
                self.snapshot.capture("extraction_output", extracted_jobs, {
                    "stage": "信息提取输出",
                    "success_count": len(extracted_jobs),
                    "failed_count": failed_count
                })
            
            return len(extracted_jobs) > 0
            
        except Exception as e:
            self.logger.error("信息提取失败", {"error": str(e)}, e)
            return False
    
    def _optimize_job_fields(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """优化岗位字段结构（16个核心字段）"""
        self.logger.debug("开始优化岗位字段结构", {
            "input_count": len(jobs),
            "target_fields": 16
        })
        
        optimized_jobs = []
        
        for job in jobs:
            optimized_job = {}
            
            # 1. 核心信息 (6个字段)
            core_fields = ["岗位名称", "公司名称", "薪资", "工作地点", "岗位描述", "岗位链接"]
            for field in core_fields:
                optimized_job[field] = job.get(field, "")
            
            # 2. 筛选评分 (2个字段) - 来自筛选系统
            optimized_job["综合评分"] = job.get("综合评分", 0)
            optimized_job["推荐等级"] = job.get("推荐等级", "⚠️ 未评分")
            
            # 3. 匹配分析 (2个字段)
            optimized_job["经验要求"] = job.get("经验要求", "")
            
            # 生成经验匹配建议
            experience_suggestion = self._generate_experience_suggestion(job)
            optimized_job["经验匹配建议"] = experience_suggestion
            
            # 4. 时间信息 (2个字段) - 使用标准化版本
            optimized_job["毕业时间要求_标准化"] = (
                job.get("毕业时间要求_标准化") or 
                job.get("毕业时间要求", "")
            )
            optimized_job["招聘截止日期_标准化"] = (
                job.get("招聘截止日期_标准化") or 
                job.get("招聘截止日期", "")
            )
            
            # 5. 补充信息 (2个字段)  
            optimized_job["发布平台"] = job.get("发布平台", "")
            optimized_job["招募方向"] = job.get("招募方向", "")
            
            # 6. HR和抓取信息 (2个字段)
            optimized_job["HR活跃度"] = job.get("HR活跃度", "")
            optimized_job["页面抓取时间"] = job.get("页面抓取时间", "")
            
            optimized_jobs.append(optimized_job)
        
        self.logger.success("岗位字段优化完成", {
            "input_count": len(jobs),
            "output_count": len(optimized_jobs),
            "fields_per_job": 16
        })
        
        return optimized_jobs
    
    def _generate_experience_suggestion(self, job: Dict[str, Any]) -> str:
        """生成经验匹配建议"""
        experience_req = job.get("经验要求", "").lower()
        score = job.get("综合评分", 0)
        
        if "应届" in experience_req or "无经验" in experience_req:
            if score >= 80:
                return "🎯 应届生岗位，强烈建议申请"
            elif score >= 60:
                return "✅ 应届生岗位，建议申请"
            else:
                return "⚠️ 应届生岗位，但匹配度一般"
        elif "1-3年" in experience_req or "1年" in experience_req:
            if score >= 80:
                return "🌟 经验要求匹配，优先申请"
            elif score >= 60:
                return "✅ 经验基本匹配，建议申请"
            else:
                return "⚠️ 可尝试申请，准备充分面试"
        elif "3-5年" in experience_req:
            return "🔄 需要更多经验，可考虑跳槽时申请"
        else:
            if score >= 70:
                return "💡 岗位优质，值得尝试申请"
            else:
                return "📋 了解岗位要求，评估申请可行性"
    
    # async def _ensure_optimized_notion_properties(self) -> bool:
    #     """确保Notion数据库有正确的属性配置"""
    #     try:
            
    #         # 这里可以添加数据库属性更新逻辑
    #         # 目前假设数据库已经正确配置
    #         self.logger.debug("Notion数据库属性配置检查完成", {
    #             "properties_count": len(optimized_properties),
    #             "core_fields": 14
    #         })
            
    #         return True
            
    #     except Exception as e:
    #         self.logger.error("Notion数据库属性配置失败", {"error": str(e)}, e)
    #         return False

    async def _batch_write_optimized_jobs(self, jobs_data: List[Dict[str, Any]], max_concurrent: int = 3) -> Dict[str, int]:
        """批量写入优化的岗位数据"""
        self.logger.info(f"开始批量写入 {len(jobs_data)} 个岗位到Notion（14字段优化版）")
        
        try:
            # 直接使用现有的OptimizedNotionJobWriter
            from src.optimized_notion_writer import OptimizedNotionJobWriter
            writer = OptimizedNotionJobWriter()
            
            # 调用现有的批量写入方法
            stats = await writer.batch_write_jobs_optimized(jobs_data, max_concurrent)
            
            self.logger.success("批量写入完成", stats)
            return stats
            
        except Exception as e:
            self.logger.error("批量写入失败", {"error": str(e)}, e)
            # 返回失败统计，格式与OptimizedNotionJobWriter一致
            return {
                "total": len(jobs_data),
                "success": 0,
                "failed": len(jobs_data),
                "strongly_recommended": 0,
                "recommended": 0,
                "considerable": 0,
                "not_recommended": 0
            }
        

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='带筛选系统的智能岗位处理流水线（修复版）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🔧 修复版特点：
  ✅ 正确处理"无新数据"情况，避免误导性的"失败"状态
  ✅ 区分"系统错误"和"无新数据"
  ✅ 提供友好的用户提示和建议

功能特点：
  🔍 分层筛选：基础筛选（硬性条件）+ 高级筛选（智能评分）
  📊 智能评分：0-100分综合评分，推荐等级自动分类
  💎 字段优化：从24个字段精简到14个核心字段
  🚫 硬性筛选：自动过滤毕业时间不符、招聘已截止的岗位
  ⭐ 推荐排序：按匹配度和公司知名度智能排序

使用模式：
  1. 完整流水线（首次运行）：
     python integrated_pipeline_with_filters.py
  
  2. 使用已有数据（推荐调试）：
     python integrated_pipeline_with_filters.py --skip-crawl --log-level debug
  
  3. 禁用筛选（仅测试）：
     python integrated_pipeline_with_filters.py --no-filters
  
  4. 极速调试模式：
     python integrated_pipeline_with_filters.py --skip-crawl --skip-notion-load --log-level trace

修复说明：
  现在当所有岗位都已存在时，系统会显示：
  ✅ 流水线执行成功 - 数据库已是最新状态
  ℹ️  这是正常情况，说明去重系统工作正常
  
  而不是误导性的"失败"消息。

示例：
  # 标准模式（推荐）
  python integrated_pipeline_with_filters.py --log-level normal
  
  # 调试模式  
  python integrated_pipeline_with_filters.py --skip-crawl --log-level debug
        """
    )
    
    parser.add_argument('--log-level', 
                      choices=['production', 'normal', 'debug', 'trace'],
                      default='normal',
                      help='日志详细程度 (默认: normal)')
    
    parser.add_argument('--no-debug-data', 
                      action='store_true',
                      help='不保存调试数据文件（节省磁盘空间）')
    
    parser.add_argument('--test-mode',
                      action='store_true', 
                      help='测试模式（限制处理数量，降低API成本）')
    
    parser.add_argument('--skip-crawl',
                      action='store_true',
                      help='跳过爬虫，使用已有数据文件')
    
    parser.add_argument('--data-file',
                      type=str,
                      help='指定要使用的数据文件路径（与--skip-crawl一起使用）')
    
    parser.add_argument('--skip-notion-load',
                      action='store_true',
                      help='跳过Notion加载，使用缓存文件（大幅提速）')
    
    parser.add_argument('--notion-cache-file',
                      type=str,
                      help='指定要使用的Notion缓存文件路径')
    
    parser.add_argument('--no-filters',
                      action='store_true',
                      help='禁用筛选功能（仅用于测试对比）')
    
    parser.add_argument('--list-notion-cache',
                      action='store_true',
                      help='列出可用的Notion缓存文件')
    
    parser.add_argument('--list-data-files',
                      action='store_true',
                      help='列出可用的数据文件')
    
    return parser.parse_args()


async def main():
    """主函数"""
    if not DEPENDENCIES_OK:
        print("❌ 依赖检查失败，请安装必需的模块")
        return
    
    # 解析命令行参数
    args = parse_args()
    
    # 如果只是列出文件，直接返回
    if args.list_data_files:
        from src.enhanced_pipeline_fixed import list_available_data_files
        list_available_data_files()
        return
    
    if args.list_notion_cache:
        from src.enhanced_pipeline_fixed import list_available_notion_cache
        list_available_notion_cache()
        return
    
    # 初始化日志系统
    log_level_map = {
        'production': LogLevel.PRODUCTION,
        'normal': LogLevel.NORMAL,
        'debug': LogLevel.DEBUG,
        'trace': LogLevel.TRACE
    }
    
    init_logger(
        level=log_level_map[args.log_level],
        save_debug_data=not args.no_debug_data
    )
    
    logger = get_logger()
    
    # 显示启动信息
    logger.info("🔧 修复版带筛选流水线启动", {
        "version": "修复版 v1.1",
        "filters_enabled": not args.no_filters,
        "log_level": args.log_level,
        "test_mode": args.test_mode,
        "skip_crawl": args.skip_crawl,
        "skip_notion_load": args.skip_notion_load,
        "python_version": f"3.{os.sys.version_info.minor}",
        "working_directory": os.getcwd()
    })
    
    # 显示筛选配置
    if not args.no_filters:
        user_config = {
            "毕业时间": os.getenv("USER_GRADUATION", "2023-12"),
            "工作经验": f"{os.getenv('USER_EXPERIENCE_YEARS', '1.0')}年",
            "目标薪资": f"{os.getenv('TARGET_SALARY', '30')}k"
        }
        logger.info("筛选配置", user_config)
        logger.info("💡 可通过环境变量 USER_GRADUATION, USER_EXPERIENCE_YEARS, TARGET_SALARY 自定义配置")
    
    # 检查环境变量
    required_env = {
        "LLM提取": ["LLM_PROVIDER", "DEEPSEEK_API_KEY"],
        "Notion写入": ["NOTION_TOKEN", "NOTION_DATABASE_ID"]
    }
    
    missing_env = []
    for category, vars in required_env.items():
        for var in vars:
            if not os.getenv(var):
                missing_env.append(f"{category}: {var}")
    
    if missing_env:
        logger.error("缺少必需的环境变量", {"missing_vars": missing_env})
        logger.info("请在.env文件中配置这些变量")
        return
    
    logger.success("环境变量检查通过")
    
    # 运行带筛选的流水线
    try:
        # 如果是测试模式，修改配置
        config = None
        if args.test_mode:
            config = {
                "crawler": {
                    "enabled_sites": ["boss_playwright"],
                    "max_pages": 1,
                    "max_jobs_test": 3  # 测试模式只处理3个岗位
                },
                "search": {
                    "default_keyword": "大模型 算法",
                    "default_city": "101010100"
                }
            }
            logger.info("启用测试模式", {"max_jobs_per_page": 3})
        
        pipeline = FilteredJobPipeline(
            config=config,
            skip_crawl=args.skip_crawl,
            data_file=args.data_file,
            skip_notion_load=args.skip_notion_load,
            notion_cache_file=args.notion_cache_file,
            enable_filters=not args.no_filters  # 筛选开关
        )
        
        success = await pipeline.run_filtered_pipeline()
        
        if success:
            logger.info("🎉 修复版流水线执行成功")
            
            # 提供使用建议
            logger.info("🔄 下次使用建议:")
            if not args.skip_crawl:
                logger.info("   # 使用本次数据进行调试（推荐）")
                logger.info("   python integrated_pipeline_with_filters.py --skip-crawl --log-level debug")
            if not args.skip_notion_load and os.getenv("NOTION_TOKEN"):
                logger.info("   # 使用Notion缓存提速")
                logger.info("   python integrated_pipeline_with_filters.py --skip-notion-load")
            if not args.no_filters:
                logger.info("   # 对比无筛选效果")
                logger.info("   python integrated_pipeline_with_filters.py --no-filters --skip-crawl")
            
        else:
            logger.error("带筛选的流水线执行失败")
            logger.info("💡 如果遇到问题，可以分步测试:")
            logger.info("   1. python unified_filter_system.py  # 测试筛选功能")
            logger.info("   2. python enhanced_extractor.py  # 测试提取功能")
            logger.info("   3. python integrated_pipeline_with_filters.py --no-filters  # 测试无筛选版本")
        
    except Exception as e:
        logger.error("主程序执行异常", {"error": str(e)}, e)
        import traceback
        traceback.print_exc()
    
    finally:
        # 清理日志系统
        try:
            cleanup_logger()
        except Exception as e:
            print(f"⚠️ 清理日志系统时出错: {e}")
            # 尝试手动保存重要信息
            try:
                print("📁 尝试保存基本调试信息...")
                # 基本的调试信息保存逻辑
            except:
                pass
        
        if not args.no_debug_data:
            logger.info("📁 调试文件已生成:")
            logger.info("   - debug/pipeline_*.log (标准日志)")
            logger.info("   - debug/debug_session_*.json (结构化调试数据)")
            logger.info("   - debug/snapshots/ (数据快照)")


if __name__ == "__main__":
    asyncio.run(main())