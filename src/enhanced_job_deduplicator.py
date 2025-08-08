# enhanced_job_deduplicator.py - 集成日志系统的增强版去重器（语法修复版）
"""
🔧 增强版岗位去重器 - 语法修复版

功能特点：
1. 🧠 智能去重策略：
   - LLM智能去重：使用大语言模型进行语义去重
   - 传统规则去重：基于URL和内容指纹的快速去重
2. 📊 完整日志系统：
   - 详细的处理日志和统计信息
   - 数据快照保存和追踪
   - 性能监控和错误处理
3. 🔧 修复版改进：
   - 正确区分"系统错误"和"无新数据"情况
   - 改进日志消息，避免误导性状态提示
   - 增强错误处理和统计报告
   - 修复所有语法错误

核心类：
- EnhancedJobDeduplicator: 主要去重器类
- EnhancedLLMJobDeduplicator: LLM智能去重实现  
- NotionJobDeduplicator: Notion数据库去重器

修复内容：
- ✅ 修复所有字符串字面量语法错误
- ✅ 改进日志消息，正确区分"系统错误"和"无新数据"
- ✅ 修复去重完成的状态说明
- ✅ 添加详细的统计信息和处理建议
- ✅ 优化性能监控和错误追踪
"""
import os
import glob
import json
from datetime import datetime
from typing import Optional, Dict, Any, List, Set, Tuple
import hashlib
import re
import asyncio
import time

# 导入日志系统
from src.logger_config import get_logger, log_function_call
from src.data_snapshot import create_snapshot_manager

try:
    from notion_client import Client
    HAS_NOTION_CLIENT = True
except ImportError:
    HAS_NOTION_CLIENT = False
    print("⚠️ Notion客户端未安装，Notion去重功能将不可用")

# 导入LLM关键词提取器
try:
    from src.llm_keyword_extractor import LLMKeywordExtractor, LLMJobDeduplicator
    HAS_LLM_EXTRACTOR = True
except ImportError:
    HAS_LLM_EXTRACTOR = False
    print("⚠️ LLM提取器未安装，智能去重功能将不可用")

class EnhancedJobDeduplicator:
    """增强版岗位去重器 - 集成详细日志系统（语法修复版）"""
    
    def __init__(self, llm_client=None, use_llm=True):
        """初始化去重器"""
        self.llm_client = llm_client
        self.use_llm = use_llm and HAS_LLM_EXTRACTOR and llm_client is not None
        
        # 日志和快照系统
        self.logger = get_logger()
        self.snapshot = create_snapshot_manager()
        
        # 初始化统计
        self.stats = {
            "total_processed": 0,
            "url_duplicates": 0,
            "content_duplicates": 0,
            "semantic_duplicates": 0,
            "unique_jobs": 0,
            "processing_time": 0.0
        }
        
        # 初始化去重策略
        if self.use_llm:
            self.logger.success("Enabled LLM smart deduplication strategy")
            self.llm_deduplicator = EnhancedLLMJobDeduplicator(llm_client)
        else:
            self.logger.info("Using traditional rule-based deduplication strategy")
            self.url_cache: Set[str] = set()
            self.fingerprint_cache: Set[str] = set()
        
        self.logger.debug("Deduplicator initialization completed", {
            "use_llm": self.use_llm,
            "has_llm_extractor": HAS_LLM_EXTRACTOR,
            "llm_client_available": llm_client is not None,
            "strategy": "LLM智能去重" if self.use_llm else "传统规则去重"
        })
    
    @log_function_call("岗位去重处理")
    async def deduplicate_jobs(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """智能去重 - 增强日志版本（语法修复版）"""
        if not jobs:
            self.logger.warning("Input job list is empty")
            return []
        
        start_time = time.time()
        
        self.logger.debug("Starting deduplication processing", {
            "input_count": len(jobs),
            "use_llm": self.use_llm,
            "dedup_strategy": "LLM smart deduplication" if self.use_llm else "Traditional rule-based deduplication"
        })
        
        # 捕获输入数据快照
        self.snapshot.capture("local_dedup_input", jobs, {
            "stage": "Local deduplication input",
            "strategy": "LLM智能去重" if self.use_llm else "传统规则去重",
            "input_count": len(jobs)
        })
        
        try:
            # 执行去重
            if self.use_llm:
                result = await self.llm_deduplicator.deduplicate_jobs(jobs)
                self.stats = self.llm_deduplicator.get_stats()
            else:
                result = self._deduplicate_jobs_traditional(jobs)
            
            # 计算处理时间
            self.stats["processing_time"] = time.time() - start_time
            
            # 捕获输出数据快照
            self.snapshot.capture("local_dedup_output", result, {
                "stage": "Local deduplication output",
                "removed_count": len(jobs) - len(result),
                "processing_time": self.stats["processing_time"]
            })
            
            # 修复: 改进去重结果日志消息
            self._log_dedup_results(jobs, result)
            
            return result
            
        except Exception as e:
            self.logger.error("Deduplication processing failed", {
                "error": str(e),
                "input_count": len(jobs),
                "processing_time": time.time() - start_time
            }, e)
            # 发生错误时返回原始数据，避免数据丢失
            return jobs
    
    def _log_dedup_results(self, input_jobs: List[Dict], result_jobs: List[Dict]):
        """记录去重结果（修复版）"""
        input_count = len(input_jobs)
        output_count = len(result_jobs)
        removed_count = input_count - output_count
        dedup_rate = (removed_count / input_count * 100) if input_count > 0 else 0
        
        # 修复: 改进消息内容，提供更准确的状态描述
        if output_count == 0:
            result_message = "去重处理完成 - 所有岗位都是重复的"
            status = "无唯一岗位"
            suggestion = "可能原因：1) 数据源重复度高 2) 去重策略过于严格"
        elif output_count == input_count:
            result_message = "去重处理完成 - 所有岗位都是唯一的"
            status = "全部唯一"
            suggestion = "数据质量良好，无重复岗位"
        else:
            result_message = f"去重处理完成 - 去除了{removed_count}个重复岗位"
            status = "部分去重"
            suggestion = f"去重效果正常，保留了{output_count}个唯一岗位"
        
        log_data = {
            "input_count": input_count,
            "output_count": output_count,
            "removed_count": removed_count,
            "dedup_rate_percent": round(dedup_rate, 1),
            "url_duplicates": self.stats.get("url_duplicates", 0),
            "content_duplicates": self.stats.get("content_duplicates", 0),
            "semantic_duplicates": self.stats.get("semantic_duplicates", 0),
            "processing_time": round(self.stats.get("processing_time", 0), 2),
            "status": status,
            "message": suggestion
        }
        
        # 根据结果选择合适的日志级别
        if output_count == 0:
            self.logger.info_no_data(result_message, log_data)
            self.logger.info("💡 " + suggestion)
        else:
            self.logger.success(result_message, log_data)
    
    def _deduplicate_jobs_traditional(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """传统去重方法（规则基础）"""
        unique_jobs = []
        
        self.stats["total_processed"] = len(jobs)
        
        for job in jobs:
            # URL去重
            job_url = self._extract_job_id(job.get('岗位链接', ''))
            if job_url and job_url in self.url_cache:
                self.stats["url_duplicates"] += 1
                continue
            
            # 内容指纹去重
            fingerprint = self._create_smart_fingerprint(job)
            if fingerprint in self.fingerprint_cache:
                self.stats["content_duplicates"] += 1
                continue
            
            # 添加到缓存
            if job_url:
                self.url_cache.add(job_url)
            self.fingerprint_cache.add(fingerprint)
            
            unique_jobs.append(job)
        
        self.stats["unique_jobs"] = len(unique_jobs)
        
        return unique_jobs
    
    def _extract_job_id(self, url: str) -> str:
        """从URL中提取岗位ID"""
        if not url:
            return ""
        
        # 处理不同招聘网站的URL格式
        base_url = url.split('?')[0]
        
        # Boss直聘
        if 'zhipin.com' in url:
            match = re.search(r'/job_detail/([^/.]+)', base_url)
            return match.group(1) if match else base_url.split('/')[-1] if '/' in base_url else base_url
        
        # 其他网站的通用处理
        return base_url.split('/')[-1] if '/' in base_url else base_url
    
    def _create_smart_fingerprint(self, job: Dict[str, Any]) -> str:
        """智能指纹生成"""
        company = self._normalize_company_name(job.get('公司名称', ''))
        title = self._normalize_job_title(job.get('岗位名称', ''))
        location = self._normalize_location(job.get('工作地点', ''))
        
        base_fingerprint = f"{company}_{title}_{location}"
        return hashlib.md5(base_fingerprint.encode('utf-8')).hexdigest()
    
    def _normalize_company_name(self, company: str) -> str:
        """公司名称标准化"""
        if not company:
            return ""
        
        company = company.strip()
        
        # 移除常见的公司后缀
        suffixes = [
            r'有限公司$',
            r'科技有限公司$',
            r'网络科技有限公司$',
            r'信息科技有限公司$',
            r'技术有限公司$',
            r'股份有限公司$',
            r'集团有限公司$',
            r'\(.*\)$',  # 移除括号内容
            r'（.*）$'   # 移除中文括号内容
        ]
        
        for suffix in suffixes:
            company = re.sub(suffix, '', company)
        
        return company.strip().lower()
    
    def _normalize_job_title(self, title: str) -> str:
        """岗位名称标准化"""
        if not title:
            return ""
        
        title = title.strip().lower()
        
        # 移除常见的修饰词
        removals = [
            r'（.*?）',  # 中文括号
            r'\(.*?\)',  # 英文括号
            r'【.*?】',  # 中文方括号
            r'\[.*?\]',  # 英文方括号
            r'急招',
            r'高薪',
            r'包住',
            r'五险一金'
        ]
        
        for removal in removals:
            title = re.sub(removal, '', title)
        
        return title.strip()
    
    def _normalize_location(self, location: str) -> str:
        """工作地点标准化"""
        if not location:
            return ""
        
        location = location.strip().lower()
        
        # 提取主要城市和区域
        location = re.sub(r'·.*$', '', location)  # 移除详细地址
        location = re.sub(r'-.*$', '', location)  # 移除详细描述
        
        return location.strip()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取去重统计信息"""
        return self.stats.copy()


class EnhancedLLMJobDeduplicator:
    """增强版LLM智能去重器"""
    
    def __init__(self, llm_client):
        """初始化LLM去重器"""
        self.llm_client = llm_client
        self.logger = get_logger()
        
        # 统计信息
        self.stats = {
            "total_processed": 0,
            "url_duplicates": 0,
            "content_duplicates": 0,
            "semantic_duplicates": 0,
            "unique_jobs": 0,
            "llm_calls": 0,
            "processing_time": 0.0
        }
    
    async def deduplicate_jobs(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """使用LLM进行智能去重"""
        if not jobs:
            return []
        
        start_time = time.time()
        self.stats["total_processed"] = len(jobs)
        
        self.logger.info("Starting LLM smart deduplication", {
            "input_count": len(jobs),
            "llm_provider": getattr(self.llm_client, 'provider', 'unknown')
        })
        
        try:
            # 首先进行快速的URL和基础内容去重
            quick_dedup_jobs = self._quick_deduplicate(jobs)
            
            # 然后进行LLM语义去重
            if len(quick_dedup_jobs) > 1:
                semantic_unique_jobs = await self._semantic_deduplicate(quick_dedup_jobs)
            else:
                semantic_unique_jobs = quick_dedup_jobs
            
            self.stats["unique_jobs"] = len(semantic_unique_jobs)
            self.stats["processing_time"] = time.time() - start_time
            
            self.logger.success("LLM smart deduplication completed", {
                "input_count": len(jobs),
                "output_count": len(semantic_unique_jobs),
                "url_duplicates": self.stats["url_duplicates"],
                "content_duplicates": self.stats["content_duplicates"],
                "semantic_duplicates": self.stats["semantic_duplicates"],
                "llm_calls": self.stats["llm_calls"],
                "processing_time": round(self.stats["processing_time"], 2)
            })
            
            return semantic_unique_jobs
            
        except Exception as e:
            self.logger.error("LLM deduplication failed", {"error": str(e)}, e)
            # 发生错误时回退到快速去重结果
            return quick_dedup_jobs if 'quick_dedup_jobs' in locals() else jobs
    
    def _quick_deduplicate(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """快速去重（URL + 基础内容）"""
        url_cache = set()
        fingerprint_cache = set()
        unique_jobs = []
        
        for job in jobs:
            # URL去重
            job_url = self._extract_job_id(job.get('岗位链接', ''))
            if job_url and job_url in url_cache:
                self.stats["url_duplicates"] += 1
                continue
            
            # 内容指纹去重
            fingerprint = self._create_simple_fingerprint(job)
            if fingerprint in fingerprint_cache:
                self.stats["content_duplicates"] += 1
                continue
            
            # 添加到缓存
            if job_url:
                url_cache.add(job_url)
            fingerprint_cache.add(fingerprint)
            
            unique_jobs.append(job)
        
        return unique_jobs
    
    async def _semantic_deduplicate(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """语义去重（使用LLM）"""
        if len(jobs) <= 1:
            return jobs
        
        unique_jobs = []
        
        for i, current_job in enumerate(jobs):
            is_duplicate = False
            
            # 与已确认唯一的岗位进行语义比较
            for unique_job in unique_jobs:
                if await self._are_semantically_similar(current_job, unique_job):
                    self.stats["semantic_duplicates"] += 1
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                unique_jobs.append(current_job)
        
        return unique_jobs
    
    async def _are_semantically_similar(self, job1: Dict[str, Any], job2: Dict[str, Any]) -> bool:
        """判断两个岗位是否语义相似"""
        try:
            self.stats["llm_calls"] += 1
            
            # 构建比较prompt
            prompt = self._build_comparison_prompt(job1, job2)
            
            # 调用LLM
            messages = [{"role": "user", "content": prompt}]
            response = await self.llm_client._call_llm_api(messages)
            
            # 解析结果
            if response:
                return "是" in response or "相似" in response or "重复" in response
            else:
                return False
            
        except Exception as e:
            self.logger.warning("LLM semantic comparison failed", {
                "job1": job1.get("岗位名称", "N/A"),
                "job2": job2.get("岗位名称", "N/A"),
                "error": str(e)
            })
            # 发生错误时保守处理，认为不重复
            return False
    
    def _build_comparison_prompt(self, job1: Dict[str, Any], job2: Dict[str, Any]) -> str:
        """构建岗位比较的prompt"""
        return f"""请判断以下两个岗位是否是重复的（同一个岗位）：

岗位1：
- 岗位名称：{job1.get('岗位名称', 'N/A')}
- 公司名称：{job1.get('公司名称', 'N/A')}
- 工作地点：{job1.get('工作地点', 'N/A')}
- 薪资：{job1.get('薪资', 'N/A')}

岗位2：
- 岗位名称：{job2.get('岗位名称', 'N/A')}
- 公司名称：{job2.get('公司名称', 'N/A')}
- 工作地点：{job2.get('工作地点', 'N/A')}
- 薪资：{job2.get('薪资', 'N/A')}

判断标准：
1. 公司名称完全相同或高度相似
2. 岗位名称表达相同职位
3. 工作地点相同或相近
4. 薪资范围重叠

请回答：是 或 否"""
    
    def _extract_job_id(self, url: str) -> str:
        """从URL中提取岗位ID"""
        if not url:
            return ""
        
        base_url = url.split('?')[0]
        match = re.search(r'/job_detail/([^/.]+)', base_url)
        return match.group(1) if match else base_url.split('/')[-1] if '/' in base_url else base_url
    
    def _create_simple_fingerprint(self, job: Dict[str, Any]) -> str:
        """创建简单指纹"""
        company = job.get('公司名称', '').strip().lower()
        title = job.get('岗位名称', '').strip().lower()
        location = job.get('工作地点', '').strip().lower()
        
        # 简单清理
        company = re.sub(r'有限公司$|科技有限公司$|网络科技有限公司$', '', company)
        title = re.sub(r'（.*?）|\(.*?\)', '', title)
        
        fingerprint = f"{company}_{title}_{location}"
        return hashlib.md5(fingerprint.encode('utf-8')).hexdigest()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self.stats.copy()


class NotionJobDeduplicator:
    """Notion数据库岗位去重器"""
    
    def __init__(self, notion_token: str, database_id: str, skip_notion_load: bool = False, notion_cache_file: str = None):
        """初始化Notion去重器"""
        if not HAS_NOTION_CLIENT:
            raise ImportError("Notion客户端未安装，请安装notion-client包")
        
        self.notion = Client(auth=notion_token)
        self.database_id = database_id
        self.skip_notion_load = skip_notion_load
        self.logger = get_logger()
        self.snapshot = create_snapshot_manager()

        # 处理缓存文件参数
        if skip_notion_load:
            if notion_cache_file:
                # 用户指定了文件，直接使用
                self.notion_cache_file = notion_cache_file
                self.logger.info("使用指定的缓存文件", {"file": notion_cache_file})
            else:
                # 用户没有指定文件，自动查找最新的
                self.notion_cache_file = self._find_latest_cache_file()
                if self.notion_cache_file:
                    self.logger.info("自动选择最新缓存文件", {"file": self.notion_cache_file})
                else:
                    self.logger.warning("Cache file not found, falling back to API loading mode")
        else:
            self.notion_cache_file = None
        
        # 缓存已存在的岗位
        self.existing_jobs_cache = {}
        self.cache_loaded = False
        
        self.logger.debug("Notion去重器初始化完成", {
            "database_id": database_id[:8] + "...",
            "notion_client": "已连接",
            "skip_notion_load": skip_notion_load,
            "cache_file": self.notion_cache_file or "无"
        })
    
    @log_function_call("Notion去重处理")
    async def deduplicate_against_notion(self, jobs: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict]]:
        """对比Notion数据库进行去重"""
        if not jobs:
            self.logger.warning("Input job list is empty")
            return [], []
        
        self.logger.info("Starting Notion database deduplication", {
            "input_count": len(jobs),
            "database_id": self.database_id[:8] + "..."
        })
        
        try:
            # 加载Notion中已存在的岗位
            if not self.cache_loaded:
                await self._load_existing_jobs()
            
            new_jobs = []
            duplicate_jobs = []
            
            for job in jobs:
                fingerprint = self._create_notion_fingerprint(job)
                
                if fingerprint in self.existing_jobs_cache:
                    duplicate_jobs.append(job)
                    self.logger.debug("发现重复岗位", {
                        "job_title": job.get("岗位名称", "N/A"),
                        "company": job.get("公司名称", "N/A"),
                        "fingerprint": fingerprint[:8] + "..."
                    })
                else:
                    new_jobs.append(job)
                    # 添加到缓存中，避免批量处理时的内部重复
                    self.existing_jobs_cache[fingerprint] = {
                        "岗位名称": job.get("岗位名称", ""),
                        "公司名称": job.get("公司名称", ""),
                        "添加时间": datetime.now().isoformat()
                    }
            
            # 保存去重结果快照
            self.snapshot.capture("notion_dedup_result", {
                "new_jobs": new_jobs,
                "duplicate_jobs": duplicate_jobs
            }, {
                "stage": "Notion deduplication result",
                "new_count": len(new_jobs),
                "duplicate_count": len(duplicate_jobs)
            })
            
            result_message = "Notion去重完成"
            if len(new_jobs) == 0:
                result_message += " - 所有岗位已存在于数据库"
                self.logger.info_no_data(result_message, {
                    "input_count": len(jobs),
                    "new_jobs": len(new_jobs),
                    "duplicate_jobs": len(duplicate_jobs),
                    "duplicate_rate": f"{(len(duplicate_jobs)/len(jobs)*100):.1f}%",
                    "message": "数据库已包含所有岗位，无需添加新数据"
                })
            else:
                result_message += f" - 发现{len(new_jobs)}个新岗位"
                self.logger.success(result_message, {
                    "input_count": len(jobs),
                    "new_jobs": len(new_jobs),
                    "duplicate_jobs": len(duplicate_jobs),
                    "duplicate_rate": f"{(len(duplicate_jobs)/len(jobs)*100):.1f}%"
                })
            
            return new_jobs, duplicate_jobs
            
        except Exception as e:
            self.logger.error("Notion deduplication failed", {"error": str(e)}, e)
            # 发生错误时返回所有岗位作为新岗位，避免数据丢失
            return jobs, []
    
    def _find_latest_cache_file(self) -> Optional[str]:
        """查找最新的Notion缓存文件"""
        import time
        
        cache_patterns = [
            "notion_cache_*.json",           # 当前目录
            "debug/notion_cache_*.json",     # debug目录
            "cache/notion_cache_*.json",     # cache目录
            "data/notion_cache_*.json"       # data目录
        ]
        
        all_cache_files = []
        for pattern in cache_patterns:
            all_cache_files.extend(glob.glob(pattern))
        
        if not all_cache_files:
            return None
        
        # 返回最新的文件
        latest_file = max(all_cache_files, key=os.path.getmtime)
        
        # 检查文件年龄，如果太旧则警告
        file_age = time.time() - os.path.getmtime(latest_file)
        age_hours = file_age / 3600
        
        if age_hours > 24:  # 如果文件超过24小时
            self.logger.warning("Cache file is old", {
                "file": os.path.basename(latest_file),
                "age_hours": round(age_hours, 1),
                "suggestion": "考虑重新加载以获取最新数据"
            })
        
        return latest_file
    
    async def _load_from_cache(self) -> bool:
        """从缓存文件加载数据"""
        try:
            if not os.path.exists(self.notion_cache_file):
                self.logger.warning("Cache file does not exist", {"file": self.notion_cache_file})
                return False
            
            self.logger.info("Loading Notion data from cache file", {"file": self.notion_cache_file})
            
            with open(self.notion_cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 重建指纹缓存
            self.existing_jobs_cache = {}
            loaded_count = 0
            
            for job_data in cache_data.get('jobs', []):
                fingerprint = self._create_notion_fingerprint(job_data)
                self.existing_jobs_cache[fingerprint] = job_data
                loaded_count += 1
            
            self.cache_loaded = True
            
            self.logger.success("Cache data loading completed", {
                "file": os.path.basename(self.notion_cache_file),
                "total_jobs": len(cache_data.get('jobs', [])),
                "valid_jobs": loaded_count,
                "cache_timestamp": cache_data.get('timestamp', 'unknown')
            })
            return True
            
        except Exception as e:
            self.logger.error("Cache loading failed", {"error": str(e)}, e)
            return False
        
    async def _save_to_cache(self, jobs_data: List[Dict]):
        """保存数据到缓存文件"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cache_file = f"debug/notion_cache_{timestamp}.json"
            
            # 确保目录存在
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "database_id": self.database_id,
                "total_jobs": len(jobs_data),
                "jobs": jobs_data
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info("Notion数据已保存到缓存", {
                "cache_file": cache_file,
                "jobs_count": len(jobs_data)
            })
            
        except Exception as e:
            self.logger.warning("Cache file save failed", {"error": str(e)})

    async def _load_existing_jobs(self):
        """加载Notion中已存在的岗位"""
        # 如果启用跳过模式且有缓存文件，尝试从缓存加载
        if self.skip_notion_load and self.notion_cache_file:
            if await self._load_from_cache():
                return
            else:
                self.logger.warning("Cache file loading failed, falling back to API loading mode")
        
        # 从Notion API加载数据
        await self._load_from_notion_api()

    async def _load_from_notion_api(self):
        """从Notion API加载数据"""
        try:
            self.logger.info("Starting to load job data from Notion API")
            
            all_results = []
            has_more = True
            next_cursor = None
            page_count = 0
            
            while has_more:
                query_params = {
                    "database_id": self.database_id,
                    "page_size": 100
                }
                
                if next_cursor:
                    query_params["start_cursor"] = next_cursor
                
                response = self.notion.databases.query(**query_params)
                
                all_results.extend(response["results"])
                has_more = response["has_more"]
                next_cursor = response.get("next_cursor")
                page_count += 1
                
                self.logger.debug(f"加载第{page_count}页", {
                    "page_size": len(response["results"]),
                    "total_loaded": len(all_results),
                    "has_more": has_more
                })
                
                # 避免API限流
                if has_more:
                    await asyncio.sleep(0.5)
            
            # 构建指纹缓存
            jobs_data = []
            for result in all_results:
                job_data = self._extract_job_data_from_notion(result)
                if job_data:
                    fingerprint = self._create_notion_fingerprint(job_data)
                    self.existing_jobs_cache[fingerprint] = job_data
                    jobs_data.append(job_data)
            
            # 保存到缓存文件（只在正常API加载时保存）
            if not self.skip_notion_load:
                await self._save_to_cache(jobs_data)
            
            self.cache_loaded = True
            
            self.logger.success("Notion API data loading completed", {
                "total_pages": page_count,
                "total_jobs": len(all_results),
                "valid_jobs": len(self.existing_jobs_cache)
            })
            
        except Exception as e:
            self.logger.error("Failed to load data from Notion API", {"error": str(e)}, e)
            raise
    
    def _extract_job_data_from_notion(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """从Notion结果中提取岗位数据"""
        try:
            properties = result.get("properties", {})
            job_data = {}
            
            # 提取岗位名称
            title_prop = properties.get("岗位名称", {})
            if title_prop.get("title"):
                job_data["岗位名称"] = title_prop["title"][0]["plain_text"]
            
            # 提取公司名称
            company_prop = properties.get("公司名称", {})
            if company_prop.get("rich_text"):
                job_data["公司名称"] = company_prop["rich_text"][0]["plain_text"]
            
            # 提取工作地点
            location_prop = properties.get("工作地点", {})
            if location_prop.get("rich_text"):
                job_data["工作地点"] = location_prop["rich_text"][0]["plain_text"]
            
            # 提取岗位链接
            url_prop = properties.get("岗位链接", {})
            if url_prop.get("url"):
                job_data["岗位链接"] = url_prop["url"]
            
            return job_data if any(job_data.values()) else None
            
        except Exception as e:
            self.logger.warning("Failed to extract Notion job data", {
                "notion_id": result.get("id", "N/A"),
                "error": str(e)
            })
            return None
    
    def _create_notion_fingerprint(self, job_data: Dict[str, Any]) -> str:
        """创建Notion指纹（用于去重）"""
        company = job_data.get('公司名称', '').strip().lower()
        title = job_data.get('岗位名称', '').strip().lower()
        location = job_data.get('工作地点', '').strip().lower()
        url = job_data.get('岗位链接', '').strip()
        
        # 简单清理
        company = re.sub(r'有限公司$|科技有限公司$|网络科技有限公司$', '', company)
        title = re.sub(r'（.*?）|\(.*?\)', '', title)
        location = re.sub(r'·.*$|-.*$', '', location)

        # Extract job ID from URL for better deduplication
        job_id = self._extract_job_id(url) if url else ''

        # Prioritize URL-based fingerprint, fall back to content-based
        if job_id:
            fingerprint = f"url_{job_id}"
        else:
            fingerprint = f"content_{company}_{title}_{location}"
        
        return hashlib.md5(fingerprint.encode('utf-8')).hexdigest()
    
    def _extract_job_id(self, url: str) -> str:
        """从URL中提取岗位ID"""
        if not url:
            return ""
        
        # 处理不同招聘网站的URL格式
        base_url = url.split('?')[0]
        
        # Boss直聘
        if 'zhipin.com' in url:
            match = re.search(r'/job_detail/([^/.]+)', base_url)
            return match.group(1) if match else base_url.split('/')[-1] if '/' in base_url else base_url
        
        # 其他网站的通用处理
        return base_url.split('/')[-1] if '/' in base_url else base_url
    
    async def load_existing_jobs(self):
        return await self._load_existing_jobs()

    def get_fingerprint_details(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """获取指纹生成详情（用于调试）"""
        original_company = job_data.get('公司名称', '')
        original_title = job_data.get('岗位名称', '')
        original_location = job_data.get('工作地点', '')
        
        cleaned_company = re.sub(r'有限公司$|科技有限公司$|网络科技有限公司$', '', original_company.strip().lower())
        cleaned_title = re.sub(r'（.*?）|\(.*?\)', '', original_title.strip().lower())
        cleaned_location = re.sub(r'·.*$|-.*', '', original_location.strip().lower())
        
        fingerprint = f"{cleaned_company}_{cleaned_title}_{cleaned_location}"
        
        return {
            "original": {
                "company": original_company,
                "title": original_title,
                "location": original_location
            },
            "cleaned": {
                "company": cleaned_company,
                "title": cleaned_title,
                "location": cleaned_location
            },
            "fingerprint": hashlib.md5(fingerprint.encode('utf-8')).hexdigest(),
            "fingerprint_string": fingerprint
        }


# 测试函数
async def test_enhanced_deduplicator():
    """测试增强版去重器"""
    print("🧪 测试增强版去重器")
    
    # 初始化日志系统
    from src.logger_config import init_logger, LogLevel
    init_logger(LogLevel.DEBUG, enable_file_logging=True)
    
    # 创建测试数据
    test_jobs = [
        {
            "岗位名称": "Python开发工程师",
            "公司名称": "阿里巴巴科技有限公司",
            "工作地点": "杭州·西湖区",
            "薪资": "20-35K",
            "岗位链接": "https://www.zhipin.com/job_detail/12345.html"
        },
        {
            "岗位名称": "Python开发工程师（急招）",
            "公司名称": "阿里巴巴科技有限公司",
            "工作地点": "杭州·西湖区·文三路",
            "薪资": "20-35K",
            "岗位链接": "https://www.zhipin.com/job_detail/12345.html"  # 相同URL
        },
        {
            "岗位名称": "Java开发工程师",
            "公司名称": "腾讯科技有限公司",
            "工作地点": "深圳·南山区",
            "薪资": "25-40K",
            "岗位链接": "https://www.zhipin.com/job_detail/67890.html"
        }
    ]
    
    print(f"📊 测试数据：{len(test_jobs)}个岗位")
    
    # 测试传统去重
    print("\n1️⃣ 测试传统去重")
    deduplicator = EnhancedJobDeduplicator(use_llm=False)
    traditional_result = await deduplicator.deduplicate_jobs(test_jobs.copy())
    print(f"   传统去重结果：{len(traditional_result)}个唯一岗位")
    
    # 如果有LLM客户端，测试LLM去重
    if HAS_LLM_EXTRACTOR:
        print("\n2️⃣ 测试LLM智能去重")
        try:
            from llm_client import get_llm_client
            llm_client = get_llm_client()
            llm_deduplicator = EnhancedJobDeduplicator(llm_client=llm_client, use_llm=True)
            llm_result = await llm_deduplicator.deduplicate_jobs(test_jobs.copy())
            print(f"   LLM去重结果：{len(llm_result)}个唯一岗位")
        except Exception as e:
            print(f"   LLM去重测试失败：{e}")
    
    # 测试Notion去重（如果有配置）
    if os.getenv("NOTION_TOKEN") and os.getenv("NOTION_DATABASE_ID"):
        print("\n3️⃣ 测试Notion去重")
        try:
            notion_deduplicator = NotionJobDeduplicator(
                os.getenv("NOTION_TOKEN"),
                os.getenv("NOTION_DATABASE_ID")
            )
            new_jobs, duplicate_jobs = await notion_deduplicator.deduplicate_against_notion(test_jobs.copy())
            print(f"   Notion去重结果：{len(new_jobs)}个新岗位，{len(duplicate_jobs)}个重复岗位")
        except Exception as e:
            print(f"   Notion去重测试失败：{e}")
    
    print("\n✅ 测试完成")
    print("📁 查看生成的调试文件:")
    print("   - debug/pipeline_*.log")
    print("   - debug/debug_session_*.json")
    print("   - debug/snapshots/")


if __name__ == "__main__":
    """直接运行此文件进行测试"""
    print("🔧 增强版岗位去重器 - 语法修复测试模式")
    print("=" * 60)
    
    # 运行测试
    asyncio.run(test_enhanced_deduplicator())