# enhanced_pipeline_skip_crawl.py - 支持跳过爬虫的增强版流水线
"""
在原有基础上增加：
1. --skip-crawl 跳过爬虫，使用已有数据
2. --data-file 指定数据文件
3. --skip-notion-load 跳过Notion加载，使用缓存
4. --notion-cache-file 指定Notion缓存文件
5. 自动查找最新数据文件
"""
import asyncio
import json
import os
import glob
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

# 导入日志系统
from src.logger_config import LogLevel, init_logger, get_logger, cleanup_logger
from src.data_snapshot import create_snapshot_manager

# 导入其他组件
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
    from src.enhanced_job_deduplicator import EnhancedJobDeduplicator, NotionJobDeduplicator
    DEPENDENCIES_OK = True
except ImportError as e:
    print(f"[ERROR] Dependency import failed: {e}")
    DEPENDENCIES_OK = False

class EnhancedNotionJobPipelineWithLogging:
    """支持跳过爬虫和Notion缓存复用的增强版流水线"""
    
    def __init__(self, config=None, skip_crawl=False, data_file=None, 
                 skip_notion_load=False, notion_cache_file=None):
        """初始化增强版流水线"""
        # 日志和快照系统
        self.logger = get_logger()
        self.snapshot = create_snapshot_manager()
        
        self.config = config or self._load_default_config()
        self.skip_crawl = skip_crawl
        self.data_file = data_file
        self.skip_notion_load = skip_notion_load
        self.notion_cache_file = notion_cache_file
        
        # 统计信息
        self.stats = {
            "crawled": 0,
            "deduplicated": 0,
            "extracted": 0,
            "written": 0,
            "failed": 0,
            "recommended": 0,
            "not_suitable": 0,
            "need_check": 0,
            # 去重统计
            "url_duplicates": 0,
            "content_duplicates": 0,
            "notion_duplicates": 0
        }
        
        # 初始化组件
        self.extractor = None
        self.writer = None
        self.deduplicator = None
        self.notion_deduplicator = None
        
        # 数据存储
        self.raw_jobs = []
        self.deduplicated_jobs = []
        self.extracted_jobs = []
        
        self.logger.debug("流水线初始化完成", {
            "config_keys": list(self.config.keys()),
            "skip_crawl": skip_crawl,
            "data_file": data_file,
            "skip_notion_load": skip_notion_load,
            "notion_cache_file": notion_cache_file,
            "stats_initialized": list(self.stats.keys())
        })
    
    def _load_default_config(self) -> Dict[str, Any]:
        """加载默认配置"""
        try:
            from src.config import load_config
            config = load_config()
            self.logger.debug("配置文件加载成功", {"config_source": "config.py"})
            return config
        except ImportError:
            default_config = {
                "crawler": {
                    "enabled_sites": ["boss_playwright"],
                    "max_pages": 1,
                    "max_jobs_test": 5
                },
                "search": {
                    "default_keyword": "大模型 算法",
                    "default_city": "101010100"
                }
            }
            self.logger.warning("使用默认配置", {"reason": "config.py不存在"})
            return default_config
    
    def _find_latest_notion_cache(self) -> Optional[str]:
        """查找最新的Notion缓存文件（优先使用已有快照）"""
        patterns = [
            "debug/snapshots/*_notion_cache.json",  # 优先使用快照系统的缓存
            "data/notion_cache_*.json",             # 备用：手动保存的缓存
        ]
        
        all_files = []
        for pattern in patterns:
            files = glob.glob(pattern)
            all_files.extend(files)
        
        if not all_files:
            return None
        
        # 按修改时间排序，返回最新的
        latest_file = max(all_files, key=os.path.getmtime)
        return latest_file
    
    def _load_notion_cache_from_file(self, cache_file: str) -> Optional[Dict[str, Any]]:
        """从文件加载Notion缓存"""
        if not os.path.exists(cache_file):
            self.logger.error(f"Notion缓存文件不存在: {cache_file}")
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 验证缓存数据结构
            if not isinstance(cache_data, dict):
                raise ValueError("缓存文件格式错误：应为JSON对象")
            
            # 验证必需字段
            required_fields = ["existing_urls", "existing_fingerprints"]
            for field in required_fields:
                if field not in cache_data:
                    raise ValueError(f"缺少必需字段: {field}")
                if not isinstance(cache_data[field], list):
                    raise ValueError(f"{field}应为数组格式")
            
            self.logger.success("Notion缓存文件加载成功", {
                "cache_file": cache_file,
                "urls_count": len(cache_data["existing_urls"]),
                "fingerprints_count": len(cache_data["existing_fingerprints"]),
                "file_size_kb": round(os.path.getsize(cache_file) / 1024, 2),
                "file_age_hours": round((datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))).total_seconds() / 3600, 1)
            })
            
            return cache_data
            
        except Exception as e:
            self.logger.error(f"加载Notion缓存文件失败: {cache_file}", {
                "error": str(e),
                "error_type": type(e).__name__
            }, e)
            return None
    
    def _find_latest_data_file(self) -> Optional[str]:
        """查找最新的数据文件"""
        patterns = [
            "data/raw_boss_playwright_*.jsonl",
            "data/deduplicated_jobs_*.json",
            "raw_boss_playwright_*.jsonl",
            "deduplicated_jobs_*.json"
        ]
        
        all_files = []
        for pattern in patterns:
            files = glob.glob(pattern)
            all_files.extend(files)
        
        if not all_files:
            return None
        
        # 按修改时间排序，返回最新的
        latest_file = max(all_files, key=os.path.getmtime)
        return latest_file
    
    def _load_existing_data(self, file_path: str) -> List[Dict[str, Any]]:
        """加载已有的数据文件"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"数据文件不存在: {file_path}")
        
        jobs = []
        
        try:
            if file_path.endswith('.jsonl'):
                # JSONL格式（原始爬取数据）
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        if line.strip():
                            try:
                                job_data = json.loads(line.strip())
                                jobs.append(job_data)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"跳过无效行 {line_num}", {
                                    "error": str(e),
                                    "line_preview": line[:100]
                                })
            
            elif file_path.endswith('.json'):
                # JSON格式（处理过的数据）
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        jobs = data
                    else:
                        raise ValueError("JSON文件应包含岗位数组")
            
            else:
                raise ValueError(f"不支持的文件格式: {file_path}")
            
            self.logger.success(f"数据文件加载成功", {
                "file_path": file_path,
                "job_count": len(jobs),
                "file_type": "JSONL" if file_path.endswith('.jsonl') else "JSON",
                "file_size_mb": round(os.path.getsize(file_path) / 1024 / 1024, 2)
            })
            
            # 检查数据结构
            if jobs:
                sample_job = jobs[0]
                self.logger.debug("数据结构预览", {
                    "sample_keys": list(sample_job.keys()),
                    "has_html": 'html' in sample_job,
                    "has_api_data": 'api_data' in sample_job,
                    "has_url": 'url' in sample_job or '岗位链接' in sample_job
                })
            
            return jobs
            
        except Exception as e:
            self.logger.error(f"加载数据文件失败: {file_path}", {"error": str(e)}, e)
            raise
    
    async def step1_load_or_crawl_jobs(self) -> bool:
        """步骤1: 加载已有数据或爬取新数据"""
        if self.skip_crawl:
            self.logger.step_start("加载已有数据", 1, 4)
            return await self._load_existing_jobs()
        else:
            self.logger.step_start("爬取岗位数据", 1, 4)
            return await self._crawl_new_jobs()
    
    async def _load_existing_jobs(self) -> bool:
        """加载已有数据"""
        try:
            # 确定数据文件
            data_file = self.data_file
            if not data_file:
                data_file = self._find_latest_data_file()
                if not data_file:
                    self.logger.error("没有找到可用的数据文件", {
                        "searched_patterns": [
                            "data/raw_boss_playwright_*.jsonl",
                            "data/deduplicated_jobs_*.json",
                            "raw_boss_playwright_*.jsonl",
                            "deduplicated_jobs_*.json"
                        ]
                    })
                    self.logger.step_end("加载已有数据", False, {"错误": "找不到数据文件"})
                    return False
                
                self.logger.info(f"自动选择最新数据文件: {data_file}")
            
            # 加载数据
            jobs = self._load_existing_data(data_file)
            
            if not jobs:
                self.logger.error("数据文件为空")
                self.logger.step_end("加载已有数据", False, {"错误": "数据文件为空"})
                return False
            
            # 处理数据格式
            self.raw_jobs = self._normalize_job_data(jobs)
            self.stats["crawled"] = len(self.raw_jobs)
            
            # 保存数据快照
            self.snapshot.capture("loaded_data", self.raw_jobs, {
                "stage": "加载已有数据",
                "source_file": data_file,
                "file_type": "JSONL" if data_file.endswith('.jsonl') else "JSON"
            })
            
            load_success = len(self.raw_jobs) > 0
            self.logger.step_end("加载已有数据", load_success, {
                "数据文件": os.path.basename(data_file),
                "总岗位数": len(self.raw_jobs),
                "数据来源": "本地文件"
            })
            
            return load_success
            
        except Exception as e:
            self.logger.error("加载数据失败", {"error": str(e)}, e)
            self.logger.step_end("加载已有数据", False, {"错误": str(e)})
            return False
    
    def _normalize_job_data(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """标准化岗位数据格式"""
        normalized_jobs = []
        
        for job in jobs:
            normalized_job = {}
            
            # 检查数据格式并标准化
            if 'api_data' in job:
                # 原始爬取数据格式
                api_data = job.get('api_data', {})
                normalized_job = {
                    '岗位名称': api_data.get('job_name', ''),
                    '公司名称': api_data.get('company_name', ''),
                    '工作地点': api_data.get('location', ''),
                    '薪资': api_data.get('salary_desc', ''),
                    '岗位链接': job.get('url', ''),
                    '岗位描述': '',  # 需要从HTML提取
                    'html': job.get('html', ''),
                    'source_platform': job.get('source', 'Unknown'),
                    'timestamp': job.get('timestamp', '')
                }
            
            elif '岗位名称' in job:
                # 已处理数据格式
                normalized_job = job.copy()
            
            else:
                # 其他格式，尝试映射
                normalized_job = {
                    '岗位名称': job.get('job_name', job.get('title', '')),
                    '公司名称': job.get('company_name', job.get('company', '')),
                    '工作地点': job.get('location', job.get('city', '')),
                    '薪资': job.get('salary_desc', job.get('salary', '')),
                    '岗位链接': job.get('url', job.get('link', '')),
                    '岗位描述': job.get('description', ''),
                    'html': job.get('html', ''),
                    'source_platform': job.get('source_platform', job.get('source', 'Unknown')),
                    'timestamp': job.get('timestamp', '')
                }
            
            if normalized_job.get('岗位名称') or normalized_job.get('公司名称'):
                normalized_jobs.append(normalized_job)
            else:
                self.logger.warning("跳过无效数据", {"job_data": job})
        
        self.logger.debug("数据标准化完成", {
            "input_count": len(jobs),
            "output_count": len(normalized_jobs),
            "sample_normalized": normalized_jobs[0] if normalized_jobs else {}
        })
        
        return normalized_jobs
    
    async def _crawl_new_jobs(self) -> bool:
        """爬取新数据（原有逻辑）"""
        try:
            # 获取配置
            enabled_sites = self.config.get("crawler", {}).get("enabled_sites", ["boss_playwright"])
            max_pages = self.config.get("crawler", {}).get("max_pages", 1)
            max_jobs_test = self.config.get("crawler", {}).get("max_jobs_test", None)
            keyword = self.config.get("search", {}).get("default_keyword", "大模型 算法")
            city = self.config.get("search", {}).get("default_city", "101010100")
            
            crawl_params = {
                "keyword": keyword,
                "city": city,
                "max_pages": max_pages,
                "enabled_sites": enabled_sites,
                "max_jobs_test": max_jobs_test
            }
            
            self.logger.info("开始爬取岗位", crawl_params)
            
            # 加载爬虫
            crawlers = crawler_registry.load_enabled_crawlers(enabled_sites)
            
            if not crawlers:
                self.logger.error("没有可用的爬虫", {"enabled_sites": enabled_sites})
                return False
            
            self.logger.success(f"爬虫加载完成", {
                "crawler_count": len(crawlers),
                "crawler_names": list(crawlers.keys())
            })
            
            # 执行爬取
            all_jobs = []
            for platform_name, fetch_func in crawlers.items():
                self.logger.info(f"开始爬取: {platform_name}")
                
                try:
                    # 传递参数给爬虫函数
                    crawl_args = {
                        "keyword": keyword,
                        "city": city,
                        "max_pages": max_pages
                    }
                    if max_jobs_test:
                        crawl_args["max_jobs_test"] = max_jobs_test
                    
                    jobs = await fetch_func(**crawl_args)
                    
                    if jobs:
                        # 添加来源标识
                        for job in jobs:
                            if isinstance(job, dict):
                                job['source_platform'] = platform_name
                        
                        all_jobs.extend(jobs)
                        
                        self.logger.success(f"{platform_name}爬取完成", {
                            "job_count": len(jobs),
                            "platform": platform_name
                        })
                    else:
                        self.logger.warning(f"{platform_name}没有获取到岗位")
                
                except Exception as e:
                    self.logger.error(f"{platform_name}爬取失败", {
                        "platform": platform_name,
                        "error": str(e)
                    }, e)
                    continue
                
                # 平台间延迟
                await asyncio.sleep(2)
            
            # 标准化数据
            self.raw_jobs = self._normalize_job_data(all_jobs)
            self.stats["crawled"] = len(self.raw_jobs)
            
            # 保存原始数据快照
            if self.raw_jobs:
                self.snapshot.capture("raw_crawl", self.raw_jobs, {
                    "stage": "原始爬取",
                    "total_platforms": len(crawlers),
                    "crawl_params": crawl_params
                })
            
            crawl_success = len(self.raw_jobs) > 0
            self.logger.step_end("爬取岗位数据", crawl_success, {
                "总岗位数": len(self.raw_jobs),
                "爬虫数量": len(crawlers),
                "成功平台": sum(1 for jobs in [jobs for _, jobs in crawlers.items()] if jobs)
            })
            
            return crawl_success
            
        except Exception as e:
            self.logger.error("爬取步骤失败", {"error": str(e)}, e)
            self.logger.step_end("爬取岗位数据", False, {"错误": str(e)})
            return False
    
    async def step2_deduplicate_jobs(self) -> bool:
        """步骤2: 去重处理 - 模板方法（统一入口）"""
        step_name = "去重处理"
        self.logger.step_start(step_name, 2, self.get_total_steps())
        
        if not self.raw_jobs:
            self.logger.error("没有原始岗位数据")
            self.logger.step_end(step_name, False, {"错误": "没有输入数据"})
            return False
        
        try:
            # 🎯 核心去重逻辑（统一实现，URL去重修复在这里）
            deduplicated_jobs = await self._perform_core_deduplication()
            
            # 🎯 钩子方法：子类可以重写进行后处理（如筛选）
            final_jobs = await self._post_deduplication_processing(deduplicated_jobs)
            
            # 更新结果
            self.deduplicated_jobs = final_jobs
            success = len(final_jobs) > 0
            
            # 生成统计
            step_stats = self._generate_dedup_stats(deduplicated_jobs, final_jobs)
            self.logger.step_end(step_name, success, step_stats)
            
            return success
            
        except Exception as e:
            self.logger.error("去重步骤失败", {"error": str(e)}, e)
            self.logger.step_end(step_name, False, {"错误": str(e)})
            return False
    
    async def _perform_core_deduplication(self) -> List[Dict[str, Any]]:
        """核心去重逻辑 - 这里修复URL去重，只需改一次！"""
        
        # 🎯 第一阶段：本地去重（修复URL去重功能）
        self.logger.info("开始本地去重处理", {
            "input_count": len(self.raw_jobs),
            "strategy": "URL + 内容指纹 + LLM语义去重"
        })
        
        # 初始化LLM客户端
        llm_client = None
        try:
            llm_client = EnhancedNotionExtractor(config=self.config)
            self.logger.success("LLM客户端初始化成功")
        except Exception as e:
            self.logger.warning("LLM客户端初始化失败，使用基础去重", {"error": str(e)})
        
        # 🔧 关键修复：恢复本地去重器（包含URL去重）
        from src.enhanced_job_deduplicator import EnhancedJobDeduplicator
        local_deduplicator = EnhancedJobDeduplicator(
            llm_client=llm_client, 
            use_llm=bool(llm_client)
        )
        
        # 执行本地去重（包含URL去重）
        locally_deduplicated = await local_deduplicator.deduplicate_jobs(self.raw_jobs)
        local_stats = local_deduplicator.get_stats()
        
        # 保存本地去重统计
        self.stats.update({
            "url_duplicates": local_stats.get("url_duplicates", 0),
            "content_duplicates": local_stats.get("content_duplicates", 0),
            "semantic_duplicates": local_stats.get("semantic_duplicates", 0)
        })
        
        self.logger.success("本地去重完成", {
            "input_count": len(self.raw_jobs),
            "output_count": len(locally_deduplicated),
            "url_duplicates": self.stats["url_duplicates"],
            "content_duplicates": self.stats["content_duplicates"],
            "semantic_duplicates": self.stats["semantic_duplicates"]
        })
        
        # 🎯 第二阶段：Notion增量去重
        notion_token = os.getenv("NOTION_TOKEN")
        database_id = os.getenv("NOTION_DATABASE_ID")
        
        if notion_token and database_id and locally_deduplicated:
            self.logger.info("开始Notion增量去重", {
                "input_count": len(locally_deduplicated),
                "strategy": "与数据库对比去重"
            })
            
            try:
                from src.enhanced_job_deduplicator import NotionJobDeduplicator
                notion_deduplicator = NotionJobDeduplicator(
                    notion_token=notion_token,
                    database_id=database_id,
                    skip_notion_load=getattr(self, 'skip_notion_load', False),
                    notion_cache_file=getattr(self, 'notion_cache_file', None)
                )
                
                # 加载Notion中已存在的岗位
                await notion_deduplicator.load_existing_jobs()
                
                # 执行Notion去重
                new_jobs, duplicate_jobs = await notion_deduplicator.deduplicate_against_notion(locally_deduplicated)
                
                # 更新统计
                self.stats["notion_duplicates"] = len(duplicate_jobs)
                
                self.logger.success("Notion增量去重完成", {
                    "input_count": len(locally_deduplicated),
                    "new_jobs": len(new_jobs),
                    "duplicate_jobs": len(duplicate_jobs)
                })
                
                return new_jobs
                
            except Exception as e:
                self.logger.error("Notion去重失败，使用本地去重结果", {"error": str(e)}, e)
                return locally_deduplicated
        else:
            self.logger.info("跳过Notion去重，使用本地去重结果")
            return locally_deduplicated

    async def _post_deduplication_processing(self, deduplicated_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """去重后处理钩子 - 基类默认不做额外处理，子类可重写添加筛选"""
        return deduplicated_jobs
    
    def _generate_dedup_stats(self, deduplicated_jobs: List[Dict], final_jobs: List[Dict]) -> Dict[str, Any]:
        """生成去重统计信息"""
        return {
            "原始岗位": len(self.raw_jobs) if self.raw_jobs else 0,
            "本地去重后": len(deduplicated_jobs),
            "URL重复": self.stats.get('url_duplicates', 0),
            "内容重复": self.stats.get('content_duplicates', 0),
            "语义重复": self.stats.get('semantic_duplicates', 0),
            "Notion重复": self.stats.get('notion_duplicates', 0),
            "最终岗位": len(final_jobs)
        }
    
    def get_total_steps(self) -> int:
        """获取总步骤数 - 子类可重写"""
        return 4  # 基类默认4步
    
    async def step3_extract_info(self) -> bool:
        """步骤3: 增强版信息提取 - 增强日志版本"""
        self.logger.step_start("增强版信息提取", 3, 4)
        
        if not self.deduplicated_jobs:
            self.logger.error("没有去重后的岗位数据")
            self.logger.step_end("增强版信息提取", False, {"错误": "没有输入数据"})
            return False
        
        try:
            # 初始化增强版提取器
            self.extractor = EnhancedNotionExtractor(config=self.config)
            
            extraction_info = {
                "岗位数量": len(self.deduplicated_jobs),
                "用户毕业时间": "2023年12月",
                "提取器类型": "增强版LLM提取器"
            }
            
            self.logger.info("开始信息提取", extraction_info)
            
            extracted_jobs = []
            failed_count = 0
            
            for i, job in enumerate(self.deduplicated_jobs, 1):
                job_title = job.get('岗位名称', 'N/A')
                company = job.get('公司名称', 'N/A')
                
                self.logger.trace(f"提取第 {i}/{len(self.deduplicated_jobs)} 个岗位", {
                    "job_title": job_title,
                    "company": company,
                    "url": job.get('岗位链接', 'N/A')
                })
                
                try:
                    html = job.get('html', '')
                    url = job.get('岗位链接', '')
                    
                    if not html:
                        self.logger.warning(f"岗位 {i} 没有HTML内容", {
                            "job_title": job_title,
                            "company": company
                        })
                        failed_count += 1
                        continue
                    
                    # 使用增强版提取器
                    result = await self.extractor.extract_for_notion_enhanced(html, url, job)
                    
                    if result:
                        # 添加原始来源信息
                        result['source_platform'] = job.get('source_platform', 'Unknown')
                        result['原始时间戳'] = job.get('timestamp', '')
                        extracted_jobs.append(result)
                        
                        match_status = result.get('毕业时间_匹配状态', 'N/A')
                        
                        # 根据匹配状态显示不同信息并统计
                        if '符合' in match_status:
                            self.logger.success(f"提取成功【推荐】: {job_title} - {company}")
                            self.stats["recommended"] += 1
                        elif '不符合' in match_status:
                            self.logger.info(f"提取成功【不匹配】: {job_title} - {company}")
                            self.stats["not_suitable"] += 1
                        else:
                            self.logger.info(f"提取成功【需确认】: {job_title} - {company}")
                            self.stats["need_check"] += 1
                    else:
                        self.logger.warning(f"岗位 {i} 提取失败", {
                            "job_title": job_title,
                            "company": company
                        })
                        failed_count += 1
                
                except Exception as e:
                    self.logger.error(f"岗位 {i} 处理异常", {
                        "job_title": job_title,
                        "company": company,
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
                
                # 保存到文件
                await self._save_extracted_data(extracted_jobs)
            
            extraction_success = len(extracted_jobs) > 0
            
            step_stats = {
                "成功提取": len(extracted_jobs),
                "提取失败": failed_count,
                "推荐岗位": self.stats['recommended'],
                "不合适岗位": self.stats['not_suitable'],
                "需要确认": self.stats['need_check']
            }
            
            if len(extracted_jobs) + failed_count > 0:
                step_stats["成功率"] = f"{len(extracted_jobs)/(len(extracted_jobs)+failed_count)*100:.1f}%"
            
            self.logger.step_end("增强版信息提取", extraction_success, step_stats)
            
            return extraction_success
            
        except Exception as e:
            self.logger.error("信息提取步骤失败", {"error": str(e)}, e)
            self.logger.step_end("增强版信息提取", False, {"错误": str(e)})
            return False
    
    async def step4_write_to_notion(self) -> bool:
        """步骤4: 写入Notion（增强版） - 增强日志版本"""
        self.logger.step_start("写入Notion数据库（增强版）", 4, 4)
        
        if not self.extracted_jobs:
            self.logger.error("没有提取的岗位数据")
            self.logger.step_end("写入Notion数据库", False, {"错误": "没有输入数据"})
            return False
        
        try:
            # 检查Notion配置
            if not os.getenv("NOTION_TOKEN") or not os.getenv("NOTION_DATABASE_ID"):
                self.logger.error("Notion配置不完整", {
                    "notion_token_exists": bool(os.getenv("NOTION_TOKEN")),
                    "database_id_exists": bool(os.getenv("NOTION_DATABASE_ID"))
                })
                self.logger.step_end("写入Notion数据库", False, {"错误": "Notion配置不完整"})
                return False
            
            # 初始化增强版Notion写入器
            self.writer = OptimizedNotionJobWriter()
            
            # 检查数据库结构
            self.logger.info("检查Notion数据库结构")
            if not self.writer.check_database_schema():
                self.logger.error("数据库结构不完整")
                self.logger.step_end("写入Notion数据库", False, {"错误": "数据库结构不完整"})
                return False
            
            # 数据预览和分类
            recommended_jobs = [job for job in self.extracted_jobs if '符合' in job.get('毕业时间_匹配状态', '')]
            not_suitable_jobs = [job for job in self.extracted_jobs if '不符合' in job.get('毕业时间_匹配状态', '')]
            need_check_jobs = [job for job in self.extracted_jobs 
                             if job not in recommended_jobs and job not in not_suitable_jobs]
            
            write_preview = {
                "总岗位数": len(self.extracted_jobs),
                "推荐岗位": len(recommended_jobs),
                "不合适岗位": len(not_suitable_jobs),
                "需要确认": len(need_check_jobs)
            }
            
            self.logger.info("准备写入Notion", write_preview)
            
            # 显示推荐岗位预览
            if recommended_jobs:
                self.logger.info(f"发现 {len(recommended_jobs)} 个推荐岗位，建议重点关注！")
                for i, job in enumerate(recommended_jobs[:3], 1):
                    self.logger.debug(f"推荐岗位 {i}", {
                        "job_title": job.get('岗位名称', 'N/A'),
                        "company": job.get('公司名称', 'N/A'),
                        "salary": job.get('薪资', 'N/A'),
                        "deadline_status": job.get('招聘截止日期_状态', 'N/A')
                    })
            
            # 执行批量写入
            self.logger.info("开始批量写入到Notion")
            stats = await self.writer.batch_write_jobs(
                self.extracted_jobs, 
                max_concurrent=2  # 控制并发避免API限流
            )
            
            self.stats["written"] = stats["success"]
            
            # 保存最终输出快照
            if stats["success"] > 0:
                final_output = {
                    "write_stats": stats,
                    "successful_jobs": [job for job in self.extracted_jobs[:stats["success"]]],
                    "write_time": datetime.now().isoformat()
                }
                self.snapshot.capture("final_output", final_output, {
                    "stage": "最终Notion写入结果",
                    "success_count": stats["success"]
                })
            
            write_success = stats["success"] > 0
            
            step_stats = {
                "成功写入": stats["success"],
                "写入失败": stats["failed"],
                "推荐岗位": stats.get("recommended", 0),
                "不合适岗位": stats.get("not_suitable", 0),
                "需要确认": stats.get("need_check", 0)
            }
            
            if stats["total"] > 0:
                step_stats["成功率"] = f"{stats['success']/stats['total']*100:.1f}%"
            
            self.logger.step_end("写入Notion数据库", write_success, step_stats)
            
            return write_success
            
        except Exception as e:
            self.logger.error("Notion写入步骤失败", {"error": str(e)}, e)
            self.logger.step_end("写入Notion数据库", False, {"错误": str(e)})
            return False
    
    async def _save_deduplicated_data(self, deduplicated_jobs: List[Dict[str, Any]]):
        """保存去重后的数据到本地文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        os.makedirs("data", exist_ok=True)
        output_file = f"data/deduplicated_jobs_{timestamp}.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(deduplicated_jobs, f, ensure_ascii=False, indent=2)
            
            self.logger.debug("去重后数据已保存", {
                "file_path": output_file,
                "job_count": len(deduplicated_jobs)
            })
            
        except Exception as e:
            self.logger.error("保存去重数据失败", {
                "file_path": output_file,
                "error": str(e)
            }, e)
    
    async def _save_extracted_data(self, extracted_jobs: List[Dict[str, Any]]):
        """保存提取的数据到本地文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        os.makedirs("data", exist_ok=True)
        output_file = f"data/enhanced_pipeline_extracted_{timestamp}.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(extracted_jobs, f, ensure_ascii=False, indent=2)
            
            self.logger.debug("提取数据已保存", {
                "file_path": output_file,
                "job_count": len(extracted_jobs)
            })
            
        except Exception as e:
            self.logger.error("保存提取数据失败", {
                "file_path": output_file,
                "error": str(e)
            }, e)
    
    async def run_full_enhanced_pipeline_with_logging(self) -> bool:
        """运行完整的增强版流水线（含日志系统）"""
        start_time = datetime.now()
        
        pipeline_mode = "使用已有数据" if self.skip_crawl else "爬取新数据"
        self.logger.info("启动增强版Notion岗位处理流水线", {
            "pipeline_version": "增强版含智能去重",
            "pipeline_mode": pipeline_mode,
            "data_file": self.data_file if self.skip_crawl else None,
            "features": ["智能去重", "增量更新", "毕业时间匹配", "招聘截止日期", "招募方向提取"],
            "start_time": start_time.isoformat()
        })
        
        pipeline_success = True
        
        try:
            # 步骤1: 加载数据或爬取
            step1_success = await self.step1_load_or_crawl_jobs()
            if not step1_success:
                self.logger.error("流水线在数据获取步骤失败")
                return False
            
            # 步骤2: 智能去重
            step2_success = await self.step2_deduplicate_jobs()
            if not step2_success:
                self.logger.error("流水线在去重步骤失败")
                return False
            
            # 步骤3: 增强版提取
            step3_success = await self.step3_extract_info()
            if not step3_success:
                self.logger.error("流水线在信息提取步骤失败")
                return False
            
            # 步骤4: 写入Notion
            step4_success = await self.step4_write_to_notion()
            if not step4_success:
                self.logger.warning("流水线在Notion写入步骤失败，但数据已保存到本地")
                pipeline_success = False
            
        except KeyboardInterrupt:
            self.logger.warning("用户中断流水线")
            pipeline_success = False
        except Exception as e:
            self.logger.error("流水线执行失败", {"error": str(e)}, e)
            pipeline_success = False
        
        finally:
            # 输出最终统计
            end_time = datetime.now()
            duration = end_time - start_time
            
            final_stats = {
                "执行模式": pipeline_mode,
                "执行时间": f"{duration.total_seconds():.1f}秒",
                "爬取/加载岗位": self.stats['crawled'],
                "去重后剩余": self.stats['deduplicated'],
                "成功提取": self.stats['extracted'],
                "写入Notion": self.stats['written'],
                "处理失败": self.stats['failed'],
                "推荐岗位": self.stats['recommended'],
                "不合适岗位": self.stats['not_suitable'],
                "需要确认": self.stats['need_check']
            }
            
            # 去重效果统计
            total_removed = (self.stats['url_duplicates'] + 
                           self.stats['content_duplicates'] + 
                           self.stats['notion_duplicates'])
            
            if total_removed > 0:
                dedup_rate = (total_removed / self.stats['crawled']) * 100 if self.stats['crawled'] > 0 else 0
                final_stats["去重率"] = f"{dedup_rate:.1f}%"
                final_stats["URL重复"] = self.stats['url_duplicates']
                final_stats["内容重复"] = self.stats['content_duplicates']
                if self.stats['notion_duplicates'] > 0:
                    final_stats["Notion重复"] = self.stats['notion_duplicates']
            
            # 保存最终统计快照
            self.snapshot.capture("pipeline_final_stats", final_stats, {
                "stage": "流水线最终统计",
                "success": pipeline_success,
                "duration_seconds": duration.total_seconds()
            })
            
            if pipeline_success:
                self.logger.success("增强版流水线执行完成", final_stats)
                
                if self.stats['written'] > 0:
                    if self.stats['recommended'] > 0:
                        self.logger.info(f"🎉 成功！发现 {self.stats['recommended']} 个推荐岗位已添加到Notion数据库")
                        self.logger.info("💡 建议在Notion中筛选\"毕业时间匹配状态\" = \"✅ 符合\"查看推荐岗位")
                    else:
                        self.logger.info(f"✅ 成功！{self.stats['written']} 个岗位已添加到Notion数据库")
                        self.logger.info("⚠️  没有发现完全符合条件的岗位，建议查看需要确认的岗位")
                    
                    if total_removed > 0:
                        self.logger.info(f"🔄 智能去重节省了 {total_removed} 个重复岗位的处理时间")
                else:
                    self.logger.warning("没有岗位写入Notion，请检查数据或配置")
            else:
                self.logger.error("增强版流水线执行失败", final_stats)
            
            # 保存快照摘要
            self.snapshot.save_summary()
        
        return pipeline_success


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='增强版智能岗位处理流水线 - 支持使用已有数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用模式：
  1. 完整流水线（首次运行）：
     python enhanced_pipeline_skip_crawl.py
  
  2. 使用已有数据（跳过爬虫）：
     python enhanced_pipeline_skip_crawl.py --skip-crawl
  
  3. 使用缓存（跳过Notion加载）：
     python enhanced_pipeline_skip_crawl.py --skip-notion-load
  
  4. 极速调试模式（跳过爬虫+Notion加载）：
     python enhanced_pipeline_skip_crawl.py --skip-crawl --skip-notion-load --log-level trace
  
  5. 指定文件：
     python enhanced_pipeline_skip_crawl.py --skip-crawl --data-file data/my_jobs.jsonl --skip-notion-load --notion-cache-file data/my_cache.json

性能对比：
  完整流水线:     爬虫(2-5分钟) + Notion加载(30秒-2分钟) + 处理
  跳过爬虫:      Notion加载(30秒-2分钟) + 处理  
  跳过Notion:    爬虫(2-5分钟) + 处理
  极速模式:      仅处理 (几秒钟启动)  ← 推荐调试时使用

日志级别说明：
  production  - 最简洁输出，仅显示警告和错误
  normal      - 标准输出，显示主要步骤信息（默认）
  debug       - 详细调试，显示处理细节和数据统计
  trace       - 最详细追踪，显示每个岗位的处理过程

示例：
  # 标准爬取模式
  python enhanced_pipeline_skip_crawl.py
  
  # 使用已有数据 + 详细调试（推荐用于问题排查）
  python enhanced_pipeline_skip_crawl.py --skip-crawl --log-level trace
  
  # 指定特定数据文件
  python enhanced_pipeline_skip_crawl.py --skip-crawl --data-file data/my_jobs.jsonl --log-level debug
  
  # 测试模式（小数据量 + 详细日志）
  python enhanced_pipeline_skip_crawl.py --test-mode --log-level debug
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
    
    parser.add_argument('--list-notion-cache',
                      action='store_true',
                      help='列出可用的Notion缓存文件')
    
    parser.add_argument('--list-data-files',
                      action='store_true',
                      help='列出可用的数据文件')
    
    return parser.parse_args()

def list_available_notion_cache():
    """列出可用的Notion缓存文件"""
    patterns = [
        "debug/snapshots/*_notion_cache.json",
        "data/notion_cache_*.json",
        "notion_cache_*.json"
    ]
    
    print("📁 可用的Notion缓存文件:")
    found_files = []
    
    for pattern in patterns:
        files = glob.glob(pattern)
        for file in files:
            stat = os.stat(file)
            size_kb = stat.st_size / 1024
            mod_time = datetime.fromtimestamp(stat.st_mtime)
            
            file_info = {
                "path": file,
                "size_kb": size_kb,
                "modified": mod_time,
                "type": "快照缓存" if "snapshots" in file else "保存缓存"
            }
            found_files.append(file_info)
    
    if not found_files:
        print("   ❌ 没有找到可用的Notion缓存文件")
        print("   💡 请先运行一次完整流水线生成缓存文件")
        return
    
    # 按修改时间排序
    found_files.sort(key=lambda x: x["modified"], reverse=True)
    
    for i, file_info in enumerate(found_files):
        marker = "📍" if i == 0 else "  "
        print(f"{marker} {file_info['path']}")
        print(f"     类型: {file_info['type']}")
        print(f"     大小: {file_info['size_kb']:.2f} KB")
        print(f"     修改时间: {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        if i == 0:
            print(f"     ⭐ 最新文件（默认使用）")
        print()

def list_available_data_files():
    """列出可用的数据文件"""
    patterns = [
        "data/raw_boss_playwright_*.jsonl",
        "data/deduplicated_jobs_*.json",
        "data/enhanced_pipeline_extracted_*.json",
        "raw_boss_playwright_*.jsonl",
        "deduplicated_jobs_*.json"
    ]
    
    print("📁 可用的数据文件:")
    found_files = []
    
    for pattern in patterns:
        files = glob.glob(pattern)
        for file in files:
            stat = os.stat(file)
            size_mb = stat.st_size / 1024 / 1024
            mod_time = datetime.fromtimestamp(stat.st_mtime)
            
            file_info = {
                "path": file,
                "size_mb": size_mb,
                "modified": mod_time,
                "type": "原始爬取数据" if "raw_boss" in file else 
                       "去重后数据" if "deduplicated" in file else 
                       "提取后数据" if "extracted" in file else "其他"
            }
            found_files.append(file_info)
    
    if not found_files:
        print("   ❌ 没有找到可用的数据文件")
        print("   💡 请先运行爬虫生成数据文件")
        return
    
    # 按修改时间排序
    found_files.sort(key=lambda x: x["modified"], reverse=True)
    
    for i, file_info in enumerate(found_files):
        marker = "📍" if i == 0 else "  "
        print(f"{marker} {file_info['path']}")
        print(f"     类型: {file_info['type']}")
        print(f"     大小: {file_info['size_mb']:.2f} MB")
        print(f"     修改时间: {file_info['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        if i == 0:
            print(f"     ⭐ 最新文件（默认使用）")
        print()

async def main():
    """主函数"""
    if not DEPENDENCIES_OK:
        print("❌ 依赖检查失败，请安装必需的模块")
        return
    
    # 解析命令行参数
    args = parse_args()
    
    # 如果只是列出文件，直接返回
    if args.list_data_files:
        list_available_data_files()
        return
    
    if args.list_notion_cache:
        list_available_notion_cache()
        return
    
    # 验证参数组合
    if args.data_file and not args.skip_crawl:
        print("❌ --data-file 必须与 --skip-crawl 一起使用")
        return
    
    if args.notion_cache_file and not args.skip_notion_load:
        print("❌ --notion-cache-file 必须与 --skip-notion-load 一起使用")
        return
    
    # 初始化日志系统
    log_level = LogLevel(args.log_level)
    logger = init_logger(log_level, not args.no_debug_data)
    
    logger.info("系统启动", {
        "log_level": args.log_level,
        "debug_data_enabled": not args.no_debug_data,
        "test_mode": args.test_mode,
        "skip_crawl": args.skip_crawl,
        "data_file": args.data_file,
        "skip_notion_load": args.skip_notion_load,
        "notion_cache_file": args.notion_cache_file,
        "python_version": f"{os.sys.version_info.major}.{os.sys.version_info.minor}",
        "working_directory": os.getcwd()
    })
    
    # 如果跳过爬虫，显示将要使用的数据文件
    if args.skip_crawl:
        if args.data_file:
            if not os.path.exists(args.data_file):
                logger.error(f"指定的数据文件不存在: {args.data_file}")
                return
            logger.info(f"将使用指定数据文件: {args.data_file}")
        else:
            # 查找最新文件
            patterns = [
                "data/raw_boss_playwright_*.jsonl",
                "data/deduplicated_jobs_*.json", 
                "raw_boss_playwright_*.jsonl",
                "deduplicated_jobs_*.json"
            ]
            
            all_files = []
            for pattern in patterns:
                all_files.extend(glob.glob(pattern))
            
            if not all_files:
                logger.error("没有找到可用的数据文件")
                logger.info("💡 请先运行爬虫或使用 --list-data-files 查看可用文件")
                return
            
            latest_file = max(all_files, key=os.path.getmtime)
            logger.info(f"自动选择最新数据文件: {latest_file}")
            logger.info("💡 使用 --list-data-files 查看所有可用文件")
    
    # 如果跳过Notion加载，显示将要使用的缓存文件
    if args.skip_notion_load:
        if args.notion_cache_file:
            if not os.path.exists(args.notion_cache_file):
                logger.error(f"指定的Notion缓存文件不存在: {args.notion_cache_file}")
                return
            logger.info(f"将使用指定Notion缓存文件: {args.notion_cache_file}")
        else:
            # 查找最新缓存文件
            patterns = [
                "debug/snapshots/*_notion_cache.json",
                "data/notion_cache_*.json",
                "notion_cache_*.json"
            ]
            
            all_cache_files = []
            for pattern in patterns:
                all_cache_files.extend(glob.glob(pattern))
            
            if not all_cache_files:
                logger.error("没有找到可用的Notion缓存文件")
                logger.info("💡 请先运行完整流水线或使用 --list-notion-cache 查看可用文件")
                return
            
            latest_cache = max(all_cache_files, key=os.path.getmtime)
            logger.info(f"自动选择最新Notion缓存: {latest_cache}")
            logger.info("💡 使用 --list-notion-cache 查看所有可用缓存")
    
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
    
    # 运行增强版流水线
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
        
        pipeline = EnhancedNotionJobPipelineWithLogging(
            config=config,
            skip_crawl=args.skip_crawl,
            data_file=args.data_file,
            skip_notion_load=args.skip_notion_load,
            notion_cache_file=args.notion_cache_file
        )
        success = await pipeline.run_full_enhanced_pipeline_with_logging()
        
        if success:
            logger.info("流水线执行成功")
            
            # 提供使用建议
            if args.skip_crawl or args.skip_notion_load:
                logger.info("🔄 下次使用建议:")
                if not args.skip_crawl:
                    logger.info("   # 使用本次数据进行调试（推荐）")
                    logger.info("   python enhanced_pipeline_skip_crawl.py --skip-crawl --log-level trace")
                if not args.skip_notion_load and os.getenv("NOTION_TOKEN"):
                    logger.info("   # 使用Notion缓存提速")
                    logger.info("   python enhanced_pipeline_skip_crawl.py --skip-notion-load")
                logger.info("   # 极速调试模式")
                logger.info("   python enhanced_pipeline_skip_crawl.py --skip-crawl --skip-notion-load --log-level trace")
            else:
                logger.info("🔄 下次运行建议:")
                logger.info("   # 使用本次数据进行调试（推荐）")
                logger.info("   python enhanced_pipeline_skip_crawl.py --skip-crawl --log-level trace")
                logger.info("   # 使用缓存提速")
                logger.info("   python enhanced_pipeline_skip_crawl.py --skip-notion-load")
                logger.info("   # 爬取新数据")
                logger.info("   python enhanced_pipeline_skip_crawl.py")
            
            logger.info("📱 Notion使用建议:")
            logger.info("   1. 筛选\"毕业时间匹配状态\" = \"✅ 符合\"查看推荐岗位")
            logger.info("   2. 筛选\"招聘截止日期状态\" = \"✅ 未过期\"查看有效岗位")
            logger.info("   3. 根据\"招募方向\"了解技术要求")
            
        else:
            logger.error("流水线执行失败")
            logger.info("💡 如果遇到问题，可以分步执行:")
            logger.info("   1. python enhanced_job_deduplicator.py  # 测试去重功能")
            logger.info("   2. python enhanced_extractor.py  # 测试提取功能")
            logger.info("   3. python enhanced_notion_writer.py  # 测试写入功能")
        
    except Exception as e:
        logger.error("主程序执行异常", {"error": str(e)}, e)
        import traceback
        traceback.print_exc()
    
    finally:
        # 清理日志系统
        cleanup_logger()
        
        if not args.no_debug_data:
            logger.info("📁 调试文件已生成:")
            logger.info("   - debug/pipeline_*.log (标准日志)")
            logger.info("   - debug/debug_session_*.json (结构化调试数据)")
            logger.info("   - debug/snapshots/ (数据快照)")
            logger.info("   - debug/debug_session_latest.json (最新调试数据)")

if __name__ == "__main__":
    asyncio.run(main())