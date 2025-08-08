"""
基于LLM的智能关键词提取器
完全依赖LLM进行语义理解和关键词提取
"""
import httpx
import asyncio
import json
import re
import hashlib
from typing import List, Dict, Any, Optional, Set

class LLMKeywordExtractor:
    """基于LLM的关键词提取器"""
    
    def __init__(self, llm_client):
        """
        初始化LLM关键词提取器
        
        Args:
            llm_client: 已初始化的LLM客户端（如EnhancedNotionExtractor）
        """
        self.llm_client = llm_client
        self.keyword_cache = {}  # 缓存LLM结果，避免重复调用
    
    async def extract_discriminative_keywords(self, description: str) -> str:
        """提取具有区分度的关键词"""
        if not description or not description.strip():
            return ""
        
        # 检查缓存
        cache_key = hashlib.md5(description.encode()).hexdigest()
        if cache_key in self.keyword_cache:
            print(f"🔄 使用缓存的关键词提取结果")
            return self.keyword_cache[cache_key]
        
        try:
            print(f"🧠 使用LLM提取关键词...")
            keywords = await self._call_llm_for_keywords(description)
            
            # 缓存结果
            self.keyword_cache[cache_key] = keywords
            print(f"✅ LLM提取关键词: {keywords}")
            
            return keywords
            
        except Exception as e:
            print(f"⚠️  LLM关键词提取失败: {e}")
            return self._fallback_simple_extraction(description)
    
    async def _call_llm_for_keywords(self, description: str) -> str:
        """调用LLM提取关键词"""
        
        prompt = f"""请分析以下岗位描述，提取3-5个最能区分不同岗位的关键词。

岗位描述：
{description}

提取要求：
1. **优先级排序**：
   - 最高优先级：公司部门/团队名称（如"华为云"、"终端BG"、"微信团队"）
   - 高优先级：产品/平台名称（如"抖音"、"HarmonyOS"、"腾讯云"）
   - 中等优先级：业务方向（如"推荐系统"、"搜索引擎"、"广告算法"）
   - 较低优先级：特殊技术要求（如"大模型"、"计算机视觉"）

2. **区分度要求**：
   - 选择的关键词应该能有效区分不同的岗位
   - 避免过于通用的词汇（如"算法"、"开发"、"工程师"）
   - 避免基础技术栈（如"Python"、"Java"、"MySQL"）

3. **输出格式**：
   - 只返回关键词，用英文逗号分隔
   - 不要解释，不要编号
   - 关键词按重要性排序

示例：
输入：华为云AI团队招聘机器学习工程师，负责推荐系统算法开发，熟悉PyTorch
输出：华为云,AI团队,推荐系统

输入：字节跳动抖音推荐团队招聘算法工程师，负责短视频推荐算法优化
输出：抖音,推荐团队,短视频推荐

输入：腾讯微信支付团队招聘后端工程师，负责支付系统架构设计
输出：微信,支付团队,支付系统

请提取关键词："""

        messages = [{"role": "user", "content": prompt}]
        
        # 使用现有的LLM API调用
        response = await self.llm_client._call_llm_api(messages, max_retries=2)

        print(f"LLM原始返回: {response}")

        if response:
            # 清理和标准化关键词
            keywords = self._clean_llm_response(response)
            print(f"清理后关键词: {keywords}")
            return keywords
        
        raise Exception("LLM API调用失败")
    
    def _clean_llm_response(self, response: str) -> str:
        """清理LLM响应，提取纯关键词"""
        # 移除可能的解释文字
        lines = response.strip().split('\n')
        
        # 查找包含关键词的行（通常是包含逗号的行）
        keyword_line = ""
        for line in lines:
            line = line.strip()
            # 跳过明显的解释行
            if any(prefix in line for prefix in ['输出：', '关键词：', '提取：', '结果：']):
                keyword_line = re.sub(r'^[^：]*：\s*', '', line)
                break
            elif ',' in line and not any(word in line for word in ['要求', '示例', '说明']):
                keyword_line = line
                break
        
        if not keyword_line:
            # 如果没找到明显的关键词行，使用第一行
            keyword_line = lines[0] if lines else response
        
        # 清理关键词
        keywords = [kw.strip() for kw in keyword_line.split(',')]
        
        # 过滤无效关键词
        valid_keywords = []
        for kw in keywords:
            # 移除标点符号
            kw = re.sub(r'[。！？，；：""''（）【】]', '', kw)
            kw = kw.strip()
            
            # 验证关键词有效性
            if (len(kw) >= 2 and len(kw) <= 20 and 
                kw not in ['无', '无关键词', '暂无', 'N/A', 'NA'] and
                not kw.isdigit()):
                valid_keywords.append(kw)
        
        # 限制数量并返回
        return "_".join(valid_keywords[:5])
    
    def _fallback_simple_extraction(self, description: str) -> str:
        """LLM失败时的简单回退策略"""
        print(f"🔄 使用简单回退策略提取关键词")
        
        # 简单的关键词模式匹配
        patterns = [
            # 公司+部门
            r'([\u4e00-\u9fa5a-zA-Z]+(?:云|端|科技|技术)[\u4e00-\u9fa5a-zA-Z]*(?:团队|实验室|部门|事业部|BG))',
            
            # 知名产品/平台
            r'(微信|QQ|抖音|头条|淘宝|钉钉|支付宝|百度|搜索)',
            r'(HarmonyOS|TikTok|WeChat|ChatGPT|Claude)',
            
            # 业务方向
            r'([\u4e00-\u9fa5]*(?:推荐|搜索|广告|支付|风控)[\u4e00-\u9fa5]*(?:系统|平台|算法|团队))',
            
            # 技术方向
            r'(大模型|机器学习|深度学习|计算机视觉|自然语言处理|语音识别)',
        ]
        
        found_keywords = []
        for pattern in patterns:
            matches = re.findall(pattern, description, re.IGNORECASE)
            found_keywords.extend(matches)
        
        # 去重并限制数量
        unique_keywords = list(dict.fromkeys(found_keywords))  # 保持顺序去重
        return "_".join(unique_keywords[:3]) if unique_keywords else ""

class LLMJobDeduplicator:
    """基于LLM关键词的岗位去重器 - 修复版"""
    
    def __init__(self, llm_client):
        self.keyword_extractor = LLMKeywordExtractor(llm_client)
        self.url_cache = set()
        self.existing_jobs = []  # 存储已处理的岗位信息
        self.stats = {
            "total_processed": 0,
            "url_duplicates": 0,
            "semantic_duplicates": 0,  # 语义重复
            "unique_jobs": 0
        }
        
        # 语义相似度阈值
        self.similarity_threshold = 0.5  # 50%相似度
    
    async def deduplicate_jobs(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """使用LLM关键词和语义相似度进行岗位去重"""
        if not jobs:
            return []
        
        print(f"🧠 开始LLM智能去重处理 {len(jobs)} 个岗位...")
        
        unique_jobs = []
        
        for i, job in enumerate(jobs, 1):
            self.stats["total_processed"] += 1
            
            print(f"🔄 处理第 {i}/{len(jobs)} 个岗位: {job.get('岗位名称', 'N/A')}")
            
            # 第一层：URL去重（快速精确匹配）
            if await self._is_duplicate_by_url(job):
                self.stats["url_duplicates"] += 1
                print(f"   ⚠️  URL重复，跳过")
                continue
            
            # 第二层：LLM语义去重
            if await self._is_duplicate_by_semantic_similarity(job):
                self.stats["semantic_duplicates"] += 1
                print(f"   ⚠️  语义重复，跳过")
                continue
            
            # 保留唯一岗位
            unique_jobs.append(job)
            self.stats["unique_jobs"] += 1
            print(f"   ✅ 保留唯一岗位")
        
        self._print_dedup_stats()
        return unique_jobs
    
    async def _is_duplicate_by_url(self, job: Dict[str, Any]) -> bool:
        """基于URL判断重复"""
        url = job.get('岗位链接', '')
        if not url:
            return False
        
        # 清理URL，只保留核心部分
        clean_url = self._clean_url(url)
        
        if clean_url in self.url_cache:
            return True
        
        self.url_cache.add(clean_url)
        return False
    
    async def _is_duplicate_by_semantic_similarity(self, job: Dict[str, Any]) -> bool:
        """基于LLM关键词的语义相似度判断重复"""
        
        # 提取当前岗位的关键词
        job_text = self._build_job_text(job)
        current_keywords = await self.keyword_extractor.extract_discriminative_keywords(job_text)
        
        if not current_keywords:
            print(f"   ⚠️  无法提取关键词，跳过语义比较")
            return False
        
        print(f"✅ LLM提取关键词: {current_keywords}")
        
        # 与已有岗位进行语义相似度比较
        for existing_job in self.existing_jobs:
            similarity = self._calculate_semantic_similarity(
                current_keywords, 
                existing_job['keywords'],
                job,
                existing_job['job_data']
            )
            
            if similarity >= self.similarity_threshold:
                print(f"   🎯 发现语义相似岗位 (相似度: {similarity:.1%})")
                print(f"      当前: {current_keywords}")
                print(f"      已有: {existing_job['keywords']}")
                return True
        
        # 添加到已处理列表
        self.existing_jobs.append({
            'keywords': current_keywords,
            'job_data': job,
            'fingerprint': self._create_semantic_fingerprint(job, current_keywords)
        })
        
        return False
    
    def _calculate_semantic_similarity(self, keywords1: str, keywords2: str, 
                                     job1: Dict, job2: Dict) -> float:
        """计算两个岗位的语义相似度"""
        
        # 分解关键词
        kw1_set = set(kw.strip().lower() for kw in keywords1.split('_') if kw.strip())
        kw2_set = set(kw.strip().lower() for kw in keywords2.split('_') if kw.strip())
        
        # 1. 关键词交集相似度
        intersection = len(kw1_set.intersection(kw2_set))
        union = len(kw1_set.union(kw2_set))
        keyword_similarity = intersection / union if union > 0 else 0
        
        # 2. 公司相似度
        company_similarity = self._calculate_company_similarity(
            job1.get('公司名称', ''), 
            job2.get('公司名称', '')
        )
        
        # 3. 地点相似度
        location_similarity = self._calculate_location_similarity(
            job1.get('工作地点', ''),
            job2.get('工作地点', '')
        )
        
        # 4. 业务领域相似度（基于关键词中的业务词汇）
        business_similarity = self._calculate_business_similarity(kw1_set, kw2_set)
        
        # 综合相似度计算（加权平均）
        total_similarity = (
            keyword_similarity * 0.4 +      # 关键词相似度权重40%
            company_similarity * 0.3 +      # 公司相似度权重30%  
            business_similarity * 0.2 +     # 业务相似度权重20%
            location_similarity * 0.1       # 地点相似度权重10%
        )
        
        return total_similarity
    
    def _calculate_company_similarity(self, company1: str, company2: str) -> float:
        """计算公司名称相似度"""
        if not company1 or not company2:
            return 0.0
        
        # 标准化公司名称
        c1 = self._normalize_company_name(company1)
        c2 = self._normalize_company_name(company2)
        
        if c1 == c2:
            return 1.0
        
        # 检查是否包含关系（如"华为技术" vs "华为"）
        if c1 in c2 or c2 in c1:
            return 0.8
        
        return 0.0
    
    def _calculate_location_similarity(self, loc1: str, loc2: str) -> float:
        """计算地点相似度"""
        if not loc1 or not loc2:
            return 0.0
        
        # 标准化地点
        l1 = self._normalize_location(loc1)
        l2 = self._normalize_location(loc2)
        
        if l1 == l2:
            return 1.0
        
        # 检查是否同城（如"北京海淀区" vs "北京朝阳区"）
        city1 = re.sub(r'[·\s]*[^，。\s]*区.*', '', l1)
        city2 = re.sub(r'[·\s]*[^，。\s]*区.*', '', l2)
        
        if city1 and city2 and city1 == city2:
            return 0.7
        
        return 0.0
    
    def _calculate_business_similarity(self, kw1_set: Set[str], kw2_set: Set[str]) -> float:
        """计算业务领域相似度"""
        
        # 定义业务领域关键词
        business_domains = {
            'recommendation': {'推荐', '推荐系统', '个性化推荐', '推荐算法', '推荐引擎'},
            'computer_vision': {'计算机视觉', 'cv', '图像识别', '视觉ai', '图像处理'},
            'nlp': {'自然语言处理', 'nlp', '文本分析', '语音识别', '对话系统'},
            'ai_platform': {'ai团队', '人工智能', 'ai实验室', 'ai部门'},
            'cloud': {'云服务', '云计算', '云平台', '华为云', '腾讯云'},
            'mobile': {'移动端', '手机', '终端', 'app', '移动应用'}
        }
        
        # 找出每个岗位的业务领域
        domains1 = self._extract_business_domains(kw1_set, business_domains)
        domains2 = self._extract_business_domains(kw2_set, business_domains)
        
        if not domains1 or not domains2:
            return 0.0
        
        # 计算业务领域交集
        common_domains = domains1.intersection(domains2)
        total_domains = domains1.union(domains2)
        
        return len(common_domains) / len(total_domains) if total_domains else 0.0
    
    def _extract_business_domains(self, keywords: Set[str], business_domains: Dict) -> Set[str]:
        """从关键词中提取业务领域"""
        found_domains = set()
        
        for domain, domain_keywords in business_domains.items():
            for keyword in keywords:
                if any(domain_kw in keyword.lower() for domain_kw in domain_keywords):
                    found_domains.add(domain)
                    break
        
        return found_domains
    
    def _normalize_company_name(self, company: str) -> str:
        """标准化公司名称"""
        if not company:
            return ""
        
        company = company.strip()
        
        # 移除常见后缀
        suffixes = [
            r'有限公司$', r'股份有限公司$', r'科技有限公司$',
            r'技术有限公司$', r'信息技术有限公司$', r'网络科技有限公司$',
            r'\(.*?\)', r'（.*?）'  # 移除括号内容
        ]
        
        for suffix in suffixes:
            company = re.sub(suffix, '', company)
        
        return company.strip().lower()
    
    def _normalize_location(self, location: str) -> str:
        """标准化地点"""
        if not location:
            return ""
        
        location = location.strip()
        
        # 移除详细区域信息，保留主要城市和区
        location = re.sub(r'[·\s]*', '', location)  # 移除特殊分隔符
        
        return location.strip().lower()
    
    def _create_semantic_fingerprint(self, job: Dict[str, Any], keywords: str) -> str:
        """创建语义指纹（用于调试）"""
        company = self._normalize_company_name(job.get('公司名称', ''))
        
        # 提取核心业务关键词
        kw_list = [kw.strip() for kw in keywords.split('_') if kw.strip()]
        core_keywords = '_'.join(sorted(kw_list[:3]))  # 取前3个关键词并排序
        
        fingerprint = f"{company}_{core_keywords}"
        return hashlib.md5(fingerprint.encode()).hexdigest()
    
    def _build_job_text(self, job: Dict[str, Any]) -> str:
        """构建用于LLM分析的岗位文本"""
        components = []
        
        # 基础信息
        if job.get('岗位名称'):
            components.append(f"岗位：{job['岗位名称']}")
        
        if job.get('公司名称'):
            components.append(f"公司：{job['公司名称']}")
        
        if job.get('工作地点'):
            components.append(f"地点：{job['工作地点']}")
        
        # 岗位描述（最重要）
        if job.get('岗位描述'):
            components.append(f"描述：{job['岗位描述']}")
        
        return "\n".join(components)
    
    def _clean_url(self, url: str) -> str:
        """清理URL"""
        if not url:
            return ""
        
        # 移除查询参数
        base_url = url.split('?')[0]
        
        # 提取岗位ID
        match = re.search(r'/job_detail/([^/.]+)', base_url)
        return match.group(1) if match else base_url
    
    def _print_dedup_stats(self):
        """打印去重统计"""
        print(f"\n📊 LLM智能去重统计:")
        print(f"   总处理: {self.stats['total_processed']} 个")
        print(f"   URL重复: {self.stats['url_duplicates']} 个")
        print(f"   语义重复: {self.stats['semantic_duplicates']} 个")
        print(f"   保留唯一: {self.stats['unique_jobs']} 个")
        
        if self.stats['total_processed'] > 0:
            dedup_rate = ((self.stats['url_duplicates'] + self.stats['semantic_duplicates']) / 
                         self.stats['total_processed']) * 100
            print(f"   去重率: {dedup_rate:.1f}%")
    
    @property
    def stats_dict(self):
        """获取统计信息"""
        return self.stats

# 测试函数
async def test_llm_keyword_extraction():
    """测试LLM关键词提取功能"""
    print("🧪 测试LLM关键词提取功能")
    print("=" * 60)
    
    # 模拟LLM客户端（需要实际的LLM配置）
    try:
        from src.enhanced_extractor import EnhancedNotionExtractor
        llm_client = EnhancedNotionExtractor()
        
        # 创建测试数据
        test_descriptions = [
            "华为云AI团队招聘机器学习算法工程师，负责推荐系统算法开发和优化，熟悉PyTorch、TensorFlow等深度学习框架",
            "华为终端AI实验室招聘机器学习算法工程师，负责手机端AI功能开发，熟悉ARM、NPU优化",
            "字节跳动抖音推荐团队招聘算法工程师，负责短视频推荐算法优化",
            "腾讯微信支付团队招聘后端工程师，负责支付系统架构设计",
            "华为云AI团队招聘机器学习算法工程师，负责推荐系统算法开发"  # 与第一个重复
        ]
        
        extractor = LLMKeywordExtractor(llm_client)
        
        print("🔍 LLM关键词提取测试:")
        for i, desc in enumerate(test_descriptions, 1):
            print(f"\n岗位 {i}:")
            print(f"描述: {desc[:50]}...")
            
            keywords = await extractor.extract_discriminative_keywords(desc)
            print(f"关键词: {keywords}")
        
        print(f"\n✅ LLM关键词提取测试完成")
        
    except ImportError:
        print("❌ 请确保enhanced_extractor.py存在且配置正确")
    except Exception as e:
        print(f"❌ 测试失败: {e}")

if __name__ == "__main__":
    asyncio.run(test_llm_keyword_extraction())