#!/usr/bin/env python3
# comprehensive_dedup_validator.py - 去重模块全面验证器

"""
去重模块验证的全面性检查脚本
覆盖所有可能的验证维度，确保去重逻辑的正确性
"""

import json
import glob
import os
import re
import hashlib
from typing import List, Dict, Any, Set, Optional, Tuple
from datetime import datetime
from collections import Counter, defaultdict
from dataclasses import dataclass

@dataclass
class ValidationResult:
    """验证结果数据类"""
    passed: bool
    details: Dict[str, Any]
    issues: List[str]
    recommendations: List[str]

class ComprehensiveDeduplicationValidator:
    """去重模块全面验证器"""
    
    def __init__(self, session_id=None):
        """初始化验证器"""
        self.snapshots_dir = "debug/snapshots"
        
        # 验证结果存储
        self.validation_results = {}
        self.global_issues = []
        self.global_recommendations = []

        self.session_id = session_id or self.find_latest_session()
        
        print(f"🚀 去重模块全面验证器启动")
        print(f"📁 会话ID: {self.session_id}")
        print(f"📂 快照目录: {self.snapshots_dir}")
        print("=" * 80)
    
    def find_latest_session(self):
        """查找最新的会话ID"""
        pattern = f"{self.snapshots_dir}/*_summary.json"
        summary_files = glob.glob(pattern)
        if not summary_files:
            raise FileNotFoundError("❌ 没有找到快照文件，请先运行流水线")
        
        latest_file = max(summary_files, key=os.path.getmtime)
        basename = os.path.basename(latest_file)
        session_id = basename.split('_summary.json')[0]
        return session_id
    
    def load_snapshot(self, stage: str) -> Optional[Any]:
        """加载指定阶段的快照"""
        pattern = f"{self.snapshots_dir}/{self.session_id}_{stage}.json"
        files = glob.glob(pattern)
        
        if not files:
            return None
            
        try:
            with open(files[0], 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  加载快照失败 {stage}: {e}")
            return None
    
    # ========== 1. 数据完整性验证 ==========
    
    def validate_data_integrity(self) -> ValidationResult:
        """验证数据完整性和流转正确性"""
        print("🔍 1. 数据完整性验证")
        print("-" * 50)
        
        issues = []
        details = {}
        
        # 加载所有快照
        snapshots = {
            "raw_crawl": self.load_snapshot("raw_crawl"),
            "local_dedup_input": self.load_snapshot("local_dedup_input"),
            "local_dedup_output": self.load_snapshot("local_dedup_output"),
            "notion_dedup_output": self.load_snapshot("notion_dedup_output"),
            "notion_cache": self.load_snapshot("notion_cache")
        }
        
        # 检查关键快照是否存在
        critical_snapshots = ["local_dedup_input", "local_dedup_output"]
        missing_snapshots = [name for name in critical_snapshots if snapshots[name] is None]
        
        if missing_snapshots:
            issues.append(f"缺少关键快照: {', '.join(missing_snapshots)}")
        
        # 统计数据量
        for name, data in snapshots.items():
            if data is not None:
                if isinstance(data, list):
                    count = len(data)
                elif isinstance(data, dict) and "existing_urls" in data:
                    count = len(data.get("existing_urls", []))
                else:
                    count = "未知格式"
                details[f"{name}_count"] = count
                print(f"   {name}: {count} 条记录")
        
        # 验证数据流转逻辑
        if snapshots["local_dedup_input"] and snapshots["local_dedup_output"]:
            input_count = len(snapshots["local_dedup_input"])
            output_count = len(snapshots["local_dedup_output"])
            
            if output_count > input_count:
                issues.append(f"本地去重输出({output_count})大于输入({input_count})，逻辑异常")
            
            details["local_dedup_ratio"] = output_count / input_count if input_count > 0 else 0
        
        if snapshots["local_dedup_output"] and snapshots["notion_dedup_output"]:
            local_count = len(snapshots["local_dedup_output"])
            notion_count = len(snapshots["notion_dedup_output"])
            
            if notion_count > local_count:
                issues.append(f"Notion去重输出({notion_count})大于本地输出({local_count})，逻辑异常")
            
            details["notion_dedup_ratio"] = notion_count / local_count if local_count > 0 else 0
        
        recommendations = []
        if not issues:
            recommendations.append("数据完整性良好")
        else:
            recommendations.append("检查流水线执行过程是否有异常")
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 2. URL去重正确性验证 ==========
    
    def validate_url_deduplication(self) -> ValidationResult:
        """验证URL去重的正确性"""
        print("\n🔍 2. URL去重正确性验证")
        print("-" * 50)
        
        issues = []
        details = {}
        recommendations = []
        
        input_data = self.load_snapshot("local_dedup_input")
        output_data = self.load_snapshot("local_dedup_output")
        
        if not input_data or not output_data:
            issues.append("缺少必要的去重数据")
            return ValidationResult(False, details, issues, recommendations)
        
        # 提取和分析URL
        input_urls = [job.get('岗位链接', '') for job in input_data if job.get('岗位链接')]
        output_urls = [job.get('岗位链接', '') for job in output_data if job.get('岗位链接')]
        
        # 清理URL（与去重逻辑保持一致）
        def clean_url(url):
            if not url:
                return ""
            base_url = url.split('?')[0].split('#')[0]
            match = re.search(r'/job_detail/([^/.]+)', base_url)
            return match.group(1) if match else base_url.split('/')[-1] if '/' in base_url else base_url
        
        clean_input_urls = [clean_url(url) for url in input_urls]
        clean_output_urls = [clean_url(url) for url in output_urls]
        
        # 统计信息
        details.update({
            "input_total_urls": len(input_urls),
            "input_unique_urls": len(set(clean_input_urls)),
            "output_total_urls": len(output_urls),
            "output_unique_urls": len(set(clean_output_urls)),
            "url_duplicates_in_input": len(input_urls) - len(set(clean_input_urls)),
            "url_duplicates_in_output": len(output_urls) - len(set(clean_output_urls))
        })
        
        print(f"   输入URL总数: {details['input_total_urls']}")
        print(f"   输入唯一URL: {details['input_unique_urls']}")
        print(f"   输出URL总数: {details['output_total_urls']}")
        print(f"   输出唯一URL: {details['output_unique_urls']}")
        
        # 验证输出中是否还有重复URL
        if details["url_duplicates_in_output"] > 0:
            issues.append(f"输出中仍有 {details['url_duplicates_in_output']} 个重复URL")
            
            # 找出重复的URL
            url_counts = Counter(clean_output_urls)
            duplicates = {url: count for url, count in url_counts.items() if count > 1}
            details["duplicate_urls"] = list(duplicates.keys())[:5]  # 只记录前5个
            
            print(f"   ❌ 重复URL示例: {details['duplicate_urls']}")
            recommendations.append("检查URL清理和去重逻辑")
        else:
            print(f"   ✅ 输出中无重复URL")
            recommendations.append("URL去重工作正常")
        
        # 验证URL清理效果
        invalid_urls = [url for url in output_urls if not url or url.startswith('http') == False]
        if invalid_urls:
            issues.append(f"发现 {len(invalid_urls)} 个无效URL")
            details["invalid_urls"] = invalid_urls[:3]
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 3. 内容去重正确性验证 ==========
    
    def validate_content_deduplication(self) -> ValidationResult:
        """验证内容去重的正确性"""
        print("\n🔍 3. 内容去重正确性验证")
        print("-" * 50)
        
        issues = []
        details = {}
        recommendations = []
        
        input_data = self.load_snapshot("local_dedup_input")
        output_data = self.load_snapshot("local_dedup_output")
        
        if not input_data or not output_data:
            issues.append("缺少必要的去重数据")
            return ValidationResult(False, details, issues, recommendations)
        
        # 创建内容指纹
        def create_content_fingerprint(job):
            company = job.get('公司名称', '').strip().lower()
            title = job.get('岗位名称', '').strip().lower()
            location = job.get('工作地点', '').strip().lower()
            
            # 清理公司名称
            company = re.sub(r'有限公司$|科技有限公司$|技术有限公司$', '', company)
            # 清理地点
            location = re.sub(r'[·\s]*[^，。\s]*区', '', location)
            
            return f"{company}_{title}_{location}"
        
        # 分析输入数据的内容重复
        input_fingerprints = [create_content_fingerprint(job) for job in input_data]
        output_fingerprints = [create_content_fingerprint(job) for job in output_data]
        
        input_fp_counts = Counter(input_fingerprints)
        output_fp_counts = Counter(output_fingerprints)
        
        # 统计信息
        details.update({
            "input_total_jobs": len(input_data),
            "input_unique_content": len(set(input_fingerprints)),
            "output_total_jobs": len(output_data),
            "output_unique_content": len(set(output_fingerprints)),
            "content_duplicates_in_input": len(input_data) - len(set(input_fingerprints)),
            "content_duplicates_in_output": len(output_data) - len(set(output_fingerprints))
        })
        
        print(f"   输入岗位总数: {details['input_total_jobs']}")
        print(f"   输入唯一内容: {details['input_unique_content']}")
        print(f"   输出岗位总数: {details['output_total_jobs']}")
        print(f"   输出唯一内容: {details['output_unique_content']}")
        
        # 验证输出中是否还有内容重复
        if details["content_duplicates_in_output"] > 0:
            issues.append(f"输出中仍有 {details['content_duplicates_in_output']} 个内容重复")
            
            # 找出重复的内容
            duplicate_fps = {fp: count for fp, count in output_fp_counts.items() if count > 1}
            details["duplicate_content_count"] = len(duplicate_fps)
            
            print(f"   ❌ 发现 {len(duplicate_fps)} 组重复内容")
            recommendations.append("检查内容指纹算法和去重逻辑")
            
            # 显示重复内容详情
            if duplicate_fps:
                print(f"   重复内容示例:")
                for fp, count in list(duplicate_fps.items())[:3]:
                    print(f"     - {fp} ({count}个)")
                    # 找出具体的岗位
                    matching_jobs = [job for job, job_fp in zip(output_data, output_fingerprints) if job_fp == fp]
                    for job in matching_jobs[:2]:
                        print(f"       * {job.get('岗位名称', 'N/A')} - {job.get('公司名称', 'N/A')}")
        else:
            print(f"   ✅ 输出中无内容重复")
            recommendations.append("内容去重工作正常")
        
        # 分析去重效果
        if details["content_duplicates_in_input"] > 0:
            removal_efficiency = (details["content_duplicates_in_input"] - details["content_duplicates_in_output"]) / details["content_duplicates_in_input"]
            details["content_dedup_efficiency"] = removal_efficiency
            print(f"   📊 内容去重效率: {removal_efficiency:.1%}")
            
            if removal_efficiency < 0.8:
                issues.append("内容去重效率偏低")
                recommendations.append("优化内容相似度算法")
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 4. Notion增量去重验证 ==========
    
    def validate_notion_incremental_dedup(self) -> ValidationResult:
        """验证Notion增量去重的正确性"""
        print("\n🔍 4. Notion增量去重验证")
        print("-" * 50)
        
        issues = []
        details = {}
        recommendations = []
        
        local_output = self.load_snapshot("local_dedup_output")
        notion_output = self.load_snapshot("notion_dedup_output")
        notion_cache = self.load_snapshot("notion_cache")
        
        if not local_output:
            issues.append("缺少本地去重输出数据")
        if not notion_output:
            issues.append("缺少Notion去重输出数据")
        if not notion_cache:
            issues.append("缺少Notion缓存数据")
        
        if issues:
            return ValidationResult(False, details, issues, recommendations)
        
        # 分析被Notion去重的岗位
        notion_output_urls = {job.get('岗位链接', '') for job in notion_output}
        removed_jobs = [job for job in local_output if job.get('岗位链接', '') not in notion_output_urls]
        
        details.update({
            "local_output_count": len(local_output),
            "notion_output_count": len(notion_output),
            "notion_removed_count": len(removed_jobs),
            "cached_urls_count": len(notion_cache.get("existing_urls", [])),
            "cached_fingerprints_count": len(notion_cache.get("existing_fingerprints", []))
        })
        
        print(f"   本地去重输出: {details['local_output_count']} 个岗位")
        print(f"   Notion去重输出: {details['notion_output_count']} 个岗位")
        print(f"   被Notion去重: {details['notion_removed_count']} 个岗位")
        print(f"   Notion缓存URL: {details['cached_urls_count']} 个")
        print(f"   Notion缓存指纹: {details['cached_fingerprints_count']} 个")
        
        if details["notion_removed_count"] == 0:
            print("   ✅ 没有岗位被Notion去重")
            recommendations.append("增量去重工作正常")
            return ValidationResult(True, details, issues, recommendations)
        
        # 验证被去重岗位是否真的在缓存中存在
        cached_urls = set(notion_cache.get("existing_urls", []))
        cached_fingerprints = set(notion_cache.get("existing_fingerprints", []))
        
        verification_stats = {
            "url_verified": 0,
            "fingerprint_verified": 0,
            "not_found": 0,
            "verification_errors": 0
        }
        
        print(f"\n   🔍 验证被去重岗位的真实性:")
        
        for i, job in enumerate(removed_jobs, 1):
            try:
                # URL验证
                clean_url = self._clean_url_for_cache(job.get('岗位链接', ''))
                url_found = clean_url in cached_urls
                
                # 指纹验证
                job_fingerprint = self._create_cache_fingerprint(job)
                fingerprint_found = job_fingerprint in cached_fingerprints
                
                if url_found:
                    verification_stats["url_verified"] += 1
                    print(f"     {i}. ✅ URL验证通过: {job.get('岗位名称', 'N/A')}")
                elif fingerprint_found:
                    verification_stats["fingerprint_verified"] += 1
                    print(f"     {i}. ✅ 指纹验证通过: {job.get('岗位名称', 'N/A')}")
                else:
                    verification_stats["not_found"] += 1
                    print(f"     {i}. ❌ 验证失败: {job.get('岗位名称', 'N/A')} - {job.get('公司名称', 'N/A')}")
                    issues.append(f"岗位未在缓存中找到: {job.get('岗位名称', 'N/A')}")
                    
            except Exception as e:
                verification_stats["verification_errors"] += 1
                print(f"     {i}. ⚠️  验证出错: {e}")
        
        details.update(verification_stats)
        
        # 计算验证成功率
        total_verified = verification_stats["url_verified"] + verification_stats["fingerprint_verified"]
        if details["notion_removed_count"] > 0:
            verification_rate = total_verified / details["notion_removed_count"]
            details["verification_success_rate"] = verification_rate
            print(f"   📊 验证成功率: {verification_rate:.1%}")
            
            if verification_rate < 0.9:
                issues.append(f"验证成功率偏低: {verification_rate:.1%}")
                recommendations.append("检查缓存数据完整性和去重逻辑一致性")
            else:
                recommendations.append("Notion增量去重工作正常")
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 5. 去重性能和效率验证 ==========
    
    def validate_dedup_performance(self) -> ValidationResult:
        """验证去重性能和效率"""
        print("\n🔍 5. 去重性能和效率验证")
        print("-" * 50)
        
        issues = []
        details = {}
        recommendations = []
        
        # 加载数据
        input_data = self.load_snapshot("local_dedup_input")
        output_data = self.load_snapshot("local_dedup_output")
        notion_output = self.load_snapshot("notion_dedup_output")
        
        if not all([input_data, output_data]):
            issues.append("缺少必要的性能分析数据")
            return ValidationResult(False, details, issues, recommendations)
        
        # 计算去重率
        input_count = len(input_data)
        local_output_count = len(output_data)
        final_output_count = len(notion_output) if notion_output else local_output_count
        
        local_dedup_rate = (input_count - local_output_count) / input_count if input_count > 0 else 0
        overall_dedup_rate = (input_count - final_output_count) / input_count if input_count > 0 else 0
        
        details.update({
            "input_count": input_count,
            "local_output_count": local_output_count,
            "final_output_count": final_output_count,
            "local_dedup_rate": local_dedup_rate,
            "overall_dedup_rate": overall_dedup_rate,
            "local_removed": input_count - local_output_count,
            "notion_removed": local_output_count - final_output_count if notion_output else 0
        })
        
        print(f"   输入岗位: {input_count} 个")
        print(f"   本地去重后: {local_output_count} 个 (去重率: {local_dedup_rate:.1%})")
        print(f"   最终输出: {final_output_count} 个 (总去重率: {overall_dedup_rate:.1%})")
        
        # 评估去重效率
        if overall_dedup_rate == 0:
            print("   ⚠️  没有发现任何重复岗位")
            recommendations.append("检查是否输入数据本身就没有重复")
        elif overall_dedup_rate < 0.1:
            print("   📊 去重率较低，可能输入数据质量较高")
            recommendations.append("去重效果正常")
        elif overall_dedup_rate > 0.5:
            print("   📊 去重率较高，输入数据重复度较大")
            recommendations.append("考虑优化数据源质量")
            if overall_dedup_rate > 0.8:
                issues.append("去重率过高，可能存在过度去重")
        else:
            print("   ✅ 去重率适中")
            recommendations.append("去重效果良好")
        
        # 检查数据保留质量
        if final_output_count == 0:
            issues.append("所有岗位都被去重，可能存在问题")
        elif final_output_count < input_count * 0.1:
            issues.append("保留的岗位过少，可能过度去重")
            recommendations.append("检查去重阈值设置")
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 6. 业务逻辑验证 ==========
    
    def validate_business_logic(self) -> ValidationResult:
        """验证业务逻辑的合理性"""
        print("\n🔍 6. 业务逻辑验证")
        print("-" * 50)
        
        issues = []
        details = {}
        recommendations = []
        
        # 加载数据
        input_data = self.load_snapshot("local_dedup_input")
        output_data = self.load_snapshot("local_dedup_output")
        extraction_output = self.load_snapshot("extraction_output")
        
        if not all([input_data, output_data]):
            issues.append("缺少必要的业务验证数据")
            return ValidationResult(False, details, issues, recommendations)
        
        # 验证重要岗位是否被误删
        print("   🎯 检查重要岗位保留情况:")
        
        # 定义重要公司关键词
        important_companies = ['华为', '腾讯', '阿里', '字节', '百度', '京东', '美团', '滴滴', '小米']
        high_salary_keywords = ['30k', '40k', '50k', '60k', '25k以上', '30万', '40万', '50万']
        
        # 统计重要岗位
        def is_important_job(job):
            company = job.get('公司名称', '').lower()
            salary = job.get('薪资', '').lower()
            title = job.get('岗位名称', '').lower()
            
            # 重要公司
            if any(keyword in company for keyword in important_companies):
                return True, "重要公司"
            
            # 高薪岗位
            if any(keyword in salary for keyword in high_salary_keywords):
                return True, "高薪岗位"
            
            # 高级岗位
            if any(keyword in title for keyword in ['专家', '总监', '架构师', 'tech lead', 'senior']):
                return True, "高级岗位"
            
            return False, ""
        
        important_input_jobs = []
        important_output_jobs = []
        
        for job in input_data:
            is_important, reason = is_important_job(job)
            if is_important:
                important_input_jobs.append((job, reason))
        
        for job in output_data:
            is_important, reason = is_important_job(job)
            if is_important:
                important_output_jobs.append((job, reason))
        
        details.update({
            "important_input_count": len(important_input_jobs),
            "important_output_count": len(important_output_jobs),
            "important_removal_rate": (len(important_input_jobs) - len(important_output_jobs)) / len(important_input_jobs) if important_input_jobs else 0
        })
        
        print(f"     重要岗位输入: {len(important_input_jobs)} 个")
        print(f"     重要岗位保留: {len(important_output_jobs)} 个")
        
        if len(important_input_jobs) > len(important_output_jobs):
            removed_important = len(important_input_jobs) - len(important_output_jobs)
            print(f"     ⚠️  重要岗位被去重: {removed_important} 个")
            
            if removed_important > len(important_input_jobs) * 0.3:
                issues.append(f"过多重要岗位被去重: {removed_important}个")
                recommendations.append("检查去重逻辑是否对重要岗位过于激进")
        else:
            print(f"     ✅ 重要岗位保留良好")
        
        # 验证数据质量
        print("   📊 数据质量检查:")
        
        def check_data_quality(jobs, label):
            empty_fields = defaultdict(int)
            invalid_data = []
            
            for job in jobs:
                # 检查必要字段
                required_fields = ['岗位名称', '公司名称', '岗位链接']
                for field in required_fields:
                    if not job.get(field) or job.get(field).strip() == '':
                        empty_fields[field] += 1
                
                # 检查数据格式
                if job.get('岗位链接') and not job['岗位链接'].startswith('http'):
                    invalid_data.append(f"无效URL: {job['岗位链接']}")
            
            print(f"     {label}:")
            for field, count in empty_fields.items():
                if count > 0:
                    print(f"       {field}为空: {count} 个")
                    if count > len(jobs) * 0.1:
                        issues.append(f"{label}中{field}为空的比例过高: {count}/{len(jobs)}")
            
            if invalid_data:
                print(f"       数据格式问题: {len(invalid_data)} 个")
                details[f"{label}_invalid_data"] = invalid_data[:5]
        
        check_data_quality(input_data, "输入数据")
        check_data_quality(output_data, "输出数据")
        
        # 检查提取结果的合理性
        if extraction_output:
            print("   🎓 毕业时间匹配分析:")
            
            match_stats = defaultdict(int)
            for job in extraction_output:
                match_status = job.get('毕业时间_匹配状态', '未知')
                match_stats[match_status] += 1
            
            details["graduation_match_stats"] = dict(match_stats)
            
            for status, count in match_stats.items():
                print(f"     {status}: {count} 个")
            
            # 如果没有符合条件的岗位，给出建议
            suitable_count = match_stats.get('✅ 符合', 0) + match_stats.get('符合', 0)
            if suitable_count == 0 and len(extraction_output) > 0:
                recommendations.append("没有发现符合毕业时间要求的岗位，建议调整搜索条件或毕业时间匹配逻辑")
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 7. 边界条件验证 ==========
    
    def validate_edge_cases(self) -> ValidationResult:
        """验证边界条件和异常情况处理"""
        print("\n🔍 7. 边界条件验证")
        print("-" * 50)
        
        issues = []
        details = {}
        recommendations = []
        
        input_data = self.load_snapshot("local_dedup_input")
        output_data = self.load_snapshot("local_dedup_output")
        
        if not all([input_data, output_data]):
            issues.append("缺少边界条件验证数据")
            return ValidationResult(False, details, issues, recommendations)
        
        # 检查空数据处理
        print("   🔍 空数据处理检查:")
        
        empty_field_jobs = []
        for job in output_data:
            empty_fields = []
            critical_fields = ['岗位名称', '公司名称', '岗位链接']
            
            for field in critical_fields:
                if not job.get(field) or str(job.get(field)).strip() == '':
                    empty_fields.append(field)
            
            if empty_fields:
                empty_field_jobs.append({
                    'job': job,
                    'empty_fields': empty_fields
                })
        
        if empty_field_jobs:
            print(f"     ⚠️  发现 {len(empty_field_jobs)} 个岗位有空字段")
            details["empty_field_jobs_count"] = len(empty_field_jobs)
            
            if len(empty_field_jobs) > len(output_data) * 0.1:
                issues.append("空字段岗位比例过高")
            
            # 显示示例
            for i, item in enumerate(empty_field_jobs[:3], 1):
                job = item['job']
                print(f"       {i}. {job.get('岗位名称', 'N/A')} - 缺少: {', '.join(item['empty_fields'])}")
        else:
            print("     ✅ 无空字段问题")
        
        # 检查异常字符处理
        print("   🔍 异常字符处理检查:")
        
        problematic_jobs = []
        for job in output_data:
            issues_found = []
            
            # 检查特殊字符
            for field in ['岗位名称', '公司名称']:
                value = job.get(field, '')
                if value:
                    if len(value) > 100:
                        issues_found.append(f"{field}过长({len(value)}字符)")
                    if re.search(r'[^\u4e00-\u9fa5a-zA-Z0-9\s\-_()（）]', value):
                        issues_found.append(f"{field}包含特殊字符")
            
            # 检查URL格式
            url = job.get('岗位链接', '')
            if url and not url.startswith(('http://', 'https://')):
                issues_found.append("URL格式异常")
            
            if issues_found:
                problematic_jobs.append({
                    'job': job,
                    'issues': issues_found
                })
        
        if problematic_jobs:
            print(f"     ⚠️  发现 {len(problematic_jobs)} 个岗位有格式问题")
            details["problematic_jobs_count"] = len(problematic_jobs)
            
            for i, item in enumerate(problematic_jobs[:3], 1):
                job = item['job']
                print(f"       {i}. {job.get('岗位名称', 'N/A')[:20]}... - 问题: {', '.join(item['issues'])}")
        else:
            print("     ✅ 无格式问题")
        
        # 检查数据一致性
        print("   🔍 数据一致性检查:")
        
        # 检查同一公司岗位的一致性
        company_jobs = defaultdict(list)
        for job in output_data:
            company = job.get('公司名称', '').strip()
            if company:
                company_jobs[company].append(job)
        
        inconsistent_companies = []
        for company, jobs in company_jobs.items():
            if len(jobs) > 1:
                # 检查同一公司不同岗位的地点是否合理
                locations = [job.get('工作地点', '') for job in jobs]
                unique_locations = set(loc for loc in locations if loc)
                
                if len(unique_locations) > 3:  # 同一公司超过3个不同地点可能有问题
                    inconsistent_companies.append({
                        'company': company,
                        'job_count': len(jobs),
                        'locations': list(unique_locations)
                    })
        
        if inconsistent_companies:
            print(f"     ⚠️  发现 {len(inconsistent_companies)} 个公司地点分布异常")
            details["inconsistent_companies"] = len(inconsistent_companies)
            
            for item in inconsistent_companies[:3]:
                print(f"       - {item['company']}: {item['job_count']}个岗位, {len(item['locations'])}个地点")
        else:
            print("     ✅ 公司数据一致性良好")
        
        # 生成建议
        if empty_field_jobs:
            recommendations.append("增强数据清洗逻辑，处理空字段问题")
        if problematic_jobs:
            recommendations.append("添加数据格式验证和清理")
        if inconsistent_companies:
            recommendations.append("检查爬虫数据质量，可能存在数据污染")
        
        if not any([empty_field_jobs, problematic_jobs, inconsistent_companies]):
            recommendations.append("边界条件处理良好")
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 8. 一致性验证 ==========
    
    def validate_consistency(self) -> ValidationResult:
        """验证系统内部一致性"""
        print("\n🔍 8. 系统一致性验证")
        print("-" * 50)
        
        issues = []
        details = {}
        recommendations = []
        
        # 检查快照之间的数据一致性
        print("   🔄 快照数据一致性:")
        
        local_input = self.load_snapshot("local_dedup_input")
        local_output = self.load_snapshot("local_dedup_output")
        notion_output = self.load_snapshot("notion_dedup_output")
        
        if not all([local_input, local_output]):
            issues.append("缺少一致性验证数据")
            return ValidationResult(False, details, issues, recommendations)
        
        # 验证数据流转的一致性
        input_urls = {job.get('岗位链接', '') for job in local_input}
        output_urls = {job.get('岗位链接', '') for job in local_output}
        
        # 检查输出中是否有不在输入中的URL
        extra_urls = output_urls - input_urls
        if extra_urls:
            issues.append(f"输出中发现 {len(extra_urls)} 个不在输入中的URL")
            details["extra_urls"] = list(extra_urls)[:5]
            print(f"     ❌ 发现额外URL: {len(extra_urls)} 个")
        else:
            print(f"     ✅ URL数据流转一致")
        
        # 检查去重逻辑的确定性
        print("   🎯 去重逻辑确定性:")
        
        # 如果有相同的输入，去重结果应该是确定的
        # 这里我们检查是否有相同的岗位在不同处理中得到了不同的结果
        
        def create_job_signature(job):
            return f"{job.get('岗位名称', '')}_{job.get('公司名称', '')}_{job.get('岗位链接', '')}"
        
        input_signatures = {create_job_signature(job): job for job in local_input}
        output_signatures = {create_job_signature(job): job for job in local_output}
        
        # 检查是否有签名不一致但被认为是同一岗位的情况
        signature_inconsistencies = []
        for sig, input_job in input_signatures.items():
            if sig in output_signatures:
                output_job = output_signatures[sig]
                # 检查除了核心字段外的其他字段是否一致
                for field in ['工作地点', '薪资']:
                    if input_job.get(field) != output_job.get(field):
                        signature_inconsistencies.append({
                            'job_signature': sig,
                            'field': field,
                            'input_value': input_job.get(field),
                            'output_value': output_job.get(field)
                        })
        
        if signature_inconsistencies:
            print(f"     ⚠️  发现 {len(signature_inconsistencies)} 个字段不一致")
            details["signature_inconsistencies"] = len(signature_inconsistencies)
            recommendations.append("检查数据传递过程是否有字段修改")
        else:
            print(f"     ✅ 去重过程数据一致")
        
        # 检查缓存一致性
        if notion_output:
            print("   💾 缓存一致性:")
            
            notion_cache = self.load_snapshot("notion_cache")
            if notion_cache:
                cached_urls = set(notion_cache.get("existing_urls", []))
                
                # 验证被去重的岗位确实在缓存中
                local_urls = {job.get('岗位链接', '') for job in local_output}
                notion_urls = {job.get('岗位链接', '') for job in notion_output}
                removed_urls = local_urls - notion_urls
                
                cache_verified = 0
                cache_not_found = 0
                
                for url in removed_urls:
                    clean_url = self._clean_url_for_cache(url)
                    if clean_url in cached_urls:
                        cache_verified += 1
                    else:
                        cache_not_found += 1
                
                details.update({
                    "cache_verified_count": cache_verified,
                    "cache_not_found_count": cache_not_found
                })
                
                if cache_not_found > 0:
                    cache_consistency_rate = cache_verified / (cache_verified + cache_not_found)
                    print(f"     📊 缓存一致性: {cache_consistency_rate:.1%}")
                    
                    if cache_consistency_rate < 0.9:
                        issues.append(f"缓存一致性偏低: {cache_consistency_rate:.1%}")
                        recommendations.append("检查缓存更新逻辑")
                    else:
                        print(f"     ✅ 缓存一致性良好")
                else:
                    print(f"     ✅ 缓存完全一致")
        
        return ValidationResult(
            passed=len(issues) == 0,
            details=details,
            issues=issues,
            recommendations=recommendations
        )
    
    # ========== 工具方法 ==========
    
    def _clean_url_for_cache(self, url: str) -> str:
        """清理URL用于缓存比较"""
        if not url:
            return ""
        base_url = url.split('?')[0].split('#')[0]
        match = re.search(r'/job_detail/([^/.]+)', base_url)
        return match.group(1) if match else base_url.split('/')[-1] if '/' in base_url else base_url
    
    def _create_cache_fingerprint(self, job: dict) -> str:
        """创建缓存指纹"""
        company = job.get('公司名称', '').strip().lower()
        title = job.get('岗位名称', '').strip().lower()
        location = job.get('工作地点', '').strip().lower()
        
        company = re.sub(r'有限公司$|科技有限公司$', '', company)
        location = re.sub(r'[·\s]*[^，。\s]*区', '', location)
        
        fingerprint_text = f"{company}_{title}_{location}"
        return hashlib.md5(fingerprint_text.encode()).hexdigest()
    
    # ========== 主验证方法 ==========
    
    def run_comprehensive_validation(self) -> Dict[str, ValidationResult]:
        """运行全面验证"""
        print("🚀 启动去重模块全面验证")
        print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # 执行所有验证
        validation_methods = [
            ("数据完整性", self.validate_data_integrity),
            ("URL去重", self.validate_url_deduplication),
            ("内容去重", self.validate_content_deduplication),
            ("Notion增量去重", self.validate_notion_incremental_dedup),
            ("性能效率", self.validate_dedup_performance),
            ("业务逻辑", self.validate_business_logic),
            ("边界条件", self.validate_edge_cases),
            ("系统一致性", self.validate_consistency)
        ]
        
        results = {}
        
        for test_name, test_method in validation_methods:
            try:
                result = test_method()
                results[test_name] = result
                self.validation_results[test_name] = result
                
                # 收集全局问题和建议
                self.global_issues.extend(result.issues)
                self.global_recommendations.extend(result.recommendations)
                
            except Exception as e:
                error_result = ValidationResult(
                    passed=False,
                    details={"error": str(e)},
                    issues=[f"验证过程异常: {e}"],
                    recommendations=["检查验证环境和数据完整性"]
                )
                results[test_name] = error_result
                self.validation_results[test_name] = error_result
                print(f"   ❌ 验证过程出错: {e}")
        
        return results
    
    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """生成全面验证报告"""
        results = self.run_comprehensive_validation()
        
        print("\n" + "=" * 80)
        print("📊 全面验证结果汇总")
        print("=" * 80)
        
        # 统计验证结果
        passed_tests = sum(1 for result in results.values() if result.passed)
        total_tests = len(results)
        success_rate = passed_tests / total_tests if total_tests > 0 else 0
        
        print(f"验证项目: {total_tests} 项")
        print(f"通过验证: {passed_tests} 项")
        print(f"验证失败: {total_tests - passed_tests} 项")
        print(f"成功率: {success_rate:.1%}")
        print()
        
        # 详细结果
        for test_name, result in results.items():
            status = "✅ 通过" if result.passed else "❌ 失败"
            print(f"{test_name}: {status}")
            
            if result.issues:
                for issue in result.issues[:3]:  # 只显示前3个问题
                    print(f"  ⚠️  {issue}")
            
            if not result.passed and result.recommendations:
                for rec in result.recommendations[:2]:  # 只显示前2个建议
                    print(f"  💡 {rec}")
        
        # 全局评估
        print("\n" + "=" * 80)
        print("🎯 全局评估")
        print("=" * 80)
        
        if success_rate >= 0.9:
            overall_status = "✅ 优秀"
            assessment = "去重模块工作正常，可以放心使用"
        elif success_rate >= 0.7:
            overall_status = "⚠️ 良好"
            assessment = "去重模块基本正常，建议优化部分问题"
        elif success_rate >= 0.5:
            overall_status = "⚠️ 一般"
            assessment = "去重模块存在一些问题，需要重点关注和改进"
        else:
            overall_status = "❌ 需要改进"
            assessment = "去重模块存在严重问题，建议深入排查和修复"
        
        print(f"整体状态: {overall_status}")
        print(f"评估结论: {assessment}")
        
        # 优先级建议
        if self.global_issues:
            print(f"\n🔧 需要修复的问题 (前5个):")
            for i, issue in enumerate(self.global_issues[:5], 1):
                print(f"  {i}. {issue}")
        
        if self.global_recommendations:
            print(f"\n💡 改进建议 (前5个):")
            unique_recommendations = list(dict.fromkeys(self.global_recommendations))  # 去重
            for i, rec in enumerate(unique_recommendations[:5], 1):
                print(f"  {i}. {rec}")
        
        # 保存详细报告
        self._save_validation_report(results, success_rate, overall_status, assessment)
        
        return {
            "validation_results": results,
            "success_rate": success_rate,
            "overall_status": overall_status,
            "assessment": assessment,
            "global_issues": self.global_issues,
            "global_recommendations": unique_recommendations
        }
    
    def _save_validation_report(self, results: Dict[str, ValidationResult], 
                               success_rate: float, overall_status: str, assessment: str):
        """保存验证报告到文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"debug/dedup_validation_report_{timestamp}.json"
        
        # 构建报告数据
        report_data = {
            "validation_info": {
                "session_id": self.session_id,
                "validation_time": datetime.now().isoformat(),
                "validator_version": "1.0.0"
            },
            "summary": {
                "total_tests": len(results),
                "passed_tests": sum(1 for r in results.values() if r.passed),
                "success_rate": success_rate,
                "overall_status": overall_status,
                "assessment": assessment
            },
            "detailed_results": {},
            "global_issues": self.global_issues,
            "global_recommendations": list(dict.fromkeys(self.global_recommendations))
        }
        
        # 转换ValidationResult为可序列化的格式
        for test_name, result in results.items():
            report_data["detailed_results"][test_name] = {
                "passed": result.passed,
                "details": result.details,
                "issues": result.issues,
                "recommendations": result.recommendations
            }
        
        try:
            os.makedirs("debug", exist_ok=True)
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, ensure_ascii=False, indent=2)
            
            print(f"\n📋 详细验证报告已保存: {report_file}")
            
        except Exception as e:
            print(f"⚠️  保存验证报告失败: {e}")

# ========== 主程序入口 ==========

def main():
    """主程序"""
    try:
        # 创建验证器
        validator = ComprehensiveDeduplicationValidator()
        
        # 运行全面验证
        report = validator.generate_comprehensive_report()
        
        # 返回适当的退出码
        success_rate = report["success_rate"]
        if success_rate >= 0.9:
            exit_code = 0  # 成功
        elif success_rate >= 0.7:
            exit_code = 1  # 警告
        else:
            exit_code = 2  # 失败
        
        print(f"\n程序退出码: {exit_code}")
        return exit_code
        
    except FileNotFoundError as e:
        print(f"❌ 文件不存在: {e}")
        print("💡 请先运行流水线生成必要的快照数据")
        return 3
        
    except Exception as e:
        print(f"❌ 验证过程发生异常: {e}")
        import traceback
        traceback.print_exc()
        return 4

if __name__ == "__main__":
    exit(main())