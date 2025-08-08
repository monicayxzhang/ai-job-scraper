"""
增强版Notion提取器
新增功能：
1. 毕业时间要求提取
2. 招聘截止日期提取
3. 招募方向提取
4. 日期标准化处理
5. 毕业时间匹配判断
"""
import httpx
import json
import os
import re
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# 尝试加载不同路径的.env文件
for env_path in [".env", "../.env", "../../.env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"[OK] Loading environment variables: {env_path}")
        break

class EnhancedNotionExtractor:
    def __init__(self, provider=None, config=None):
        """增强版Notion提取器"""
        self.provider = provider or os.getenv("LLM_PROVIDER", "deepseek")
        self.config = config or {}
        self.api_key = None
        self.base_url = None
        self.model = None
        
        # 用户毕业信息（可配置）
        self.user_graduation = "2023-12"  # 2023年12月毕业
        
        self.temperature = self._get_config_value("temperature", 0)
        self.max_tokens = self._get_config_value("max_tokens", 1000)
        
        self._setup_provider()
    
    def _get_config_value(self, key: str, default_value):
        """获取配置值"""
        env_key = f"LLM_{key.upper()}"
        env_value = os.getenv(env_key)
        if env_value is not None:
            try:
                if isinstance(default_value, int):
                    return int(env_value)
                elif isinstance(default_value, float):
                    return float(env_value)
                else:
                    return env_value
            except ValueError:
                pass
        
        llm_config = self.config.get("llm", {})
        if key in llm_config:
            return llm_config[key]
        
        return default_value
    
    def _setup_provider(self):
        """设置API配置"""
        if self.provider == "deepseek":
            self.api_key = os.getenv("DEEPSEEK_API_KEY")
            self.base_url = "https://api.deepseek.com/v1"
            self.model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
        elif self.provider == "zhipu":
            self.api_key = os.getenv("ZHIPU_API_KEY")
            self.base_url = "https://open.bigmodel.cn/api/paas/v4"
            self.model = os.getenv("ZHIPU_MODEL", "glm-4-flash")
        elif self.provider == "siliconflow":
            self.api_key = os.getenv("SILICONFLOW_API_KEY")
            self.base_url = "https://api.siliconflow.cn/v1"
            self.model = os.getenv("SILICONFLOW_MODEL", "Qwen/Qwen2.5-7B-Instruct")
        elif self.provider == "01ai":
            self.api_key = os.getenv("LINGYIWANWU_API_KEY")
            self.base_url = "https://api.lingyiwanwu.com/v1"
            self.model = os.getenv("LINGYIWANWU_MODEL", "yi-large")
        else:  # openai
            self.api_key = os.getenv("OPENAI_API_KEY")
            self.base_url = "https://api.openai.com/v1"
            self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        
        generic_model = os.getenv("LLM_MODEL")
        if generic_model:
            self.model = generic_model
        
        if not self.api_key:
            print(f"⚠️  {self.provider.upper()}_API_KEY 未配置")
    
    def standardize_date_format(self, raw_date: str) -> str:
        """将各种日期格式标准化为 YYYY-MM-DD"""
        if not raw_date or not raw_date.strip():
            return ""
        
        # 清理文本
        text = raw_date.strip()
        
        # 处理届别格式
        if '届' in text:
            return text  # 保持届别格式 "2024届"
        
        # 处理范围格式
        if any(sep in text for sep in ['-', '到', '至', '~']):
            # 如果是时间范围，尝试解析两个日期
            for sep in ['-', '到', '至', '~']:
                if sep in text and '年' in text:
                    parts = text.split(sep)
                    if len(parts) == 2:
                        start_date = self._parse_single_date(parts[0].strip())
                        end_date = self._parse_single_date(parts[1].strip())
                        if start_date and end_date:
                            return f"{start_date}到{end_date}"
        
        # 处理单个日期
        return self._parse_single_date(text)
    
    def _parse_single_date(self, date_str: str) -> str:
        """解析单个日期字符串"""
        if not date_str:
            return ""
        
        # 移除常见前缀
        text = re.sub(r'^(截止日期|报名截止|申请截止|招聘截止|毕业时间)[：:]\s*', '', date_str)
        
        # 标准化模式
        patterns = [
            # 完整日期格式
            (r'(\d{4})[./年](\d{1,2})[./月](\d{1,2})[日]?', lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
            # 年月格式
            (r'(\d{4})[./年](\d{1,2})[月]?', lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-01"),
            # 只有年份
            (r'(\d{4})年?', lambda m: f"{m.group(1)}-01-01"),
            # 简化年份格式 (如24年)
            (r'(\d{2})[./年](\d{1,2})[./月](\d{1,2})[日]?', lambda m: f"20{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
        ]
        
        for pattern, formatter in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return formatter(match)
                except:
                    continue
        
        return date_str  # 如果无法解析，返回原文
    
    def check_graduation_eligibility(self, graduation_requirement: str) -> str:
        """检查是否符合毕业要求"""
        if not graduation_requirement:
            return "未知"
        
        req = graduation_requirement.lower()
        
        # 2024届毕业生（通常包含2023年11月-2024年8月）
        if "2024届" in graduation_requirement:
            return "✅ 符合 (2024届包含2023年12月毕业)"
        
        # 2025届毕业生
        if "2025届" in graduation_requirement:
            return "❌ 不符合 (2025届为2024年毕业)"
        
        # 2023届毕业生
        if "2023届" in graduation_requirement:
            return "❌ 不符合 (2023届为2022年毕业)"
        
        # 具体时间范围判断
        if "2023" in req and "2024" in req:
            if any(month in req for month in ["11月", "12月", "1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月"]):
                # 检查是否包含12月
                if "12月" in req or ("11月" in req and "8月" in req):
                    return "✅ 符合 (时间范围包含2023年12月)"
        
        # 如果包含应届生等关键词
        if any(keyword in req for keyword in ["应届", "校招", "毕业生"]):
            return "⚠️ 需要确认 (应届生招聘，建议查看详细要求)"
        
        return "❌ 不符合或需要人工确认"
    
    def check_deadline_status(self, deadline_date: str) -> str:
        """检查截止日期状态"""
        if not deadline_date:
            return "未知"
        
        try:
            # 解析标准化日期
            if re.match(r'\d{4}-\d{2}-\d{2}$', deadline_date):
                deadline = datetime.strptime(deadline_date, "%Y-%m-%d")
                now = datetime.now()
                
                if deadline < now:
                    return "❌ 已过期"
                elif deadline < now + timedelta(days=7):
                    return "⚠️ 即将截止"
                else:
                    return "✅ 未过期"
            else:
                return "⚠️ 日期格式异常"
        except:
            return "❌ 日期解析失败"
    
    def _extract_structured_info(self, html: str, url: str, job_data: Optional[Dict] = None) -> Dict[str, str]:
        """结构化提取：增强版本"""
        info = {
            "岗位名称": "",
            "薪资": "", 
            "工作地点": "",
            "经验要求": "",
            "发布平台": "",
            "HR活跃度": "",
            "页面抓取时间": "",
            # 新增字段
            "毕业时间要求": "",
            "招聘截止日期": "",
            "招募方向": ""
        }
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            
            # 1. 页面抓取时间 - 从原始数据获取
            if job_data:
                timestamp_fields = ['timestamp', '原始时间戳', 'crawl_time', 'created_at']
                for field in timestamp_fields:
                    if field in job_data and job_data[field]:
                        try:
                            if isinstance(job_data[field], str):
                                crawl_time = datetime.strptime(job_data[field], "%Y-%m-%d %H:%M:%S")
                                info["页面抓取时间"] = crawl_time.strftime("%Y-%m-%d")
                                print(f"📅 获取抓取时间: {info['页面抓取时间']} (来源: {field})")
                                break
                        except (ValueError, TypeError) as e:
                            print(f"⚠️  时间解析失败 {field}: {job_data[field]}, 错误: {e}")
                            continue
            
            # 2. 从URL和标题提取岗位名称
            title = soup.find('title')
            if title:
                title_text = title.get_text()
                title_match = re.search(r'「([^」]+?)(?:招聘|岗位)?」', title_text)
                if title_match:
                    info["岗位名称"] = title_match.group(1)
                    print(f"📋 从标题提取岗位名称: {info['岗位名称']}")
            
            # 3. 判断发布平台
            if 'zhipin.com' in url:
                info["发布平台"] = "Boss直聘"
                
                # HR活跃度
                activity_patterns = [
                    r'(\d+日?内活跃)', r'(刚刚活跃)', r'(今日活跃)', 
                    r'(本周活跃)', r'(\d+分钟前活跃)', r'(\d+小时前活跃)'
                ]
                for pattern in activity_patterns:
                    match = re.search(pattern, text)
                    if match:
                        info["HR活跃度"] = match.group(1)
                        break
            
            # 4. 薪资提取
            salary_patterns = [
                r'(\d+[-~到]\d+[kK万](?:·\d+薪)?)',
                r'(\d+[kK][-~到]\d+[kK](?:·\d+薪)?)',
                r'(\d+万[-~到]\d+万(?:·\d+薪)?)',
                r'(\d+[kK]\+(?:·\d+薪)?)',
                r'(\d+万\+(?:·\d+薪)?)',
                r'(\d+[-~到]\d+万/年)',
                r'(\d+[-~到]\d+元/天)',
                r'(\d+[kK万]·\d+薪)',
                r'(面议)',
                r'(\d+万(?:·\d+薪)?)',
                r'(\d+[kK](?:·\d+薪)?)'
            ]
            
            for i, pattern in enumerate(salary_patterns):
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    salary = max(matches, key=len)
                    info["薪资"] = salary
                    print(f"💰 提取薪资: {salary} (模式{i+1})")
                    if len(matches) > 1:
                        print(f"   🔍 所有匹配: {matches}")
                    break
            
            # 5. 工作地点提取（只保留城市）
            location_patterns = [
                r'(北京)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(上海)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(深圳)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(杭州)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(广州)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(成都)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(武汉)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(西安)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(南京)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(苏州)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(天津)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(重庆)(?:市|[·\s]*[^，。\s\d]*区)?(?![·\s]*\d+[-~]\d*年)',
                r'(远程办公)', r'(在家办公)', r'(全远程)', r'(Remote)'
            ]
            
            for i, pattern in enumerate(location_patterns):
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    location = matches[0]
                    location = re.sub(r'\s*\d+[-~]\d*年.*$', '', location).strip()
                    info["工作地点"] = location
                    print(f"🌍 提取地点: {location} (模式{i+1})")
                    break
            
            # 6. 经验要求提取
            exp_patterns = [
                r'(\d+[-~]\d+年工作经验)', r'(\d+[-~]\d+年经验)', r'(\d+年以上工作经验)',
                r'(\d+年以上经验)', r'(\d+\+年经验)', r'(\d+年工作经验)', r'(\d+年经验)',
                r'(应届毕业生)', r'(应届生)', r'(实习生)', r'(经验不限)',
                r'(在校/应届)', r'(校招)', r'(无经验要求)', r'(不限经验)',
                r'(面向\d+届)', r'(不限)'
            ]
            
            for pattern in exp_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    exp = matches[0]
                    if exp in ["不限", "经验不限", "无经验要求", "不限经验"]:
                        info["经验要求"] = "经验不限"
                    else:
                        info["经验要求"] = exp
                    print(f"📅 提取经验: {info['经验要求']}")
                    break
            
            # 7. 新增：毕业时间要求提取
            graduation_patterns = [
                r'面向(\d{4})届',
                r'(\d{4})届毕业生',
                r'毕业时间[：:]\s*(\d{4}年?\s*[-~到至]\s*\d{4}年?)',
                r'(\d{4}年\d{1,2}月?\s*[-~到至]\s*\d{4}年\d{1,2}月?)',
                r'(\d{4}[./年]\d{1,2}[./月]?\s*[-~到至]\s*\d{4}[./年]\d{1,2}[./月]?)',
                r'面向.*?(\d{4}年\d{1,2}月[-~到至]\d{4}年\d{1,2}月).*?毕业',
            ]
            
            for i, pattern in enumerate(graduation_patterns):
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    graduation_req = matches[0]
                    if pattern == graduation_patterns[0] or pattern == graduation_patterns[1]:
                        graduation_req = f"{graduation_req}届"
                    info["毕业时间要求"] = graduation_req
                    print(f"🎓 提取毕业时间要求: {graduation_req} (模式{i+1})")
                    break
            
            # 8. 新增：招聘截止日期提取
            deadline_patterns = [
                r'截止日期[：:]\s*(\d{4}[./年]\d{1,2}[./月]\d{1,2}[日]?)',
                r'报名截止[：:]\s*(\d{4}[./年]\d{1,2}[./月]\d{1,2}[日]?)',
                r'申请截止[：:]\s*(\d{4}[./年]\d{1,2}[./月]\d{1,2}[日]?)',
                r'招聘截止[：:]\s*(\d{4}[./年]\d{1,2}[./月]\d{1,2}[日]?)',
                r'截止时间[：:]\s*(\d{4}[./年]\d{1,2}[./月]\d{1,2}[日]?)',
            ]
            
            for i, pattern in enumerate(deadline_patterns):
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    deadline = matches[0]
                    info["招聘截止日期"] = deadline
                    print(f"⏰ 提取截止日期: {deadline} (模式{i+1})")
                    break
            
            # 9. 新增：招募方向提取（简单正则）
            direction_patterns = [
                r'招募方向[：:]\s*([^。\n]+)',
                r'方向[：:]\s*([^。\n]*方向[^。\n]*)',
                r'技术方向[：:]\s*([^。\n]+)',
                r'([^。\n]*方向[、，,][^。\n]*方向[^。\n]*)',
            ]
            
            for i, pattern in enumerate(direction_patterns):
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    direction = matches[0].strip()
                    if len(direction) > 10 and '方向' in direction:  # 确保是有意义的方向描述
                        info["招募方向"] = direction
                        print(f"🎯 提取招募方向: {direction} (模式{i+1})")
                        break
            
        except Exception as e:
            print(f"⚠️  结构化提取失败: {e}")
        
        return info
    
    def _prepare_html_for_llm(self, html: str) -> str:
        """为LLM准备HTML，重点保留岗位描述相关内容"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # 移除明显噪声
            for elem in soup(['script', 'style', 'nav', 'footer', 'header']):
                elem.decompose()
            
            # 查找主要岗位描述区域
            job_content_selectors = [
                '[class*="job-detail"]',
                '[class*="job-description"]', 
                '[class*="position-detail"]',
                '[class*="job-content"]',
                '[class*="desc"]'
            ]
            
            main_content = ""
            for selector in job_content_selectors:
                elements = soup.select(selector)
                if elements:
                    main_content = "\n".join([elem.get_text(separator='\n') for elem in elements])
                    break
            
            if not main_content:
                main_content = soup.get_text(separator='\n')
            
            # 清理和过滤
            lines = main_content.split('\n')
            cleaned_lines = []
            
            for line in lines:
                line = line.strip()
                if (len(line) > 5 and 
                    line not in cleaned_lines[-3:] and
                    not re.match(r'^[>\s•·\-\*\.]+$', line) and
                    '举报' not in line and '客服' not in line and 
                    '扫码' not in line and '微信' not in line):
                    cleaned_lines.append(line)
            
            content = '\n'.join(cleaned_lines)
            return content[:6000]
            
        except Exception as e:
            print(f"⚠️  HTML预处理失败: {e}")
            return re.sub(r'<[^>]+>', ' ', html)[:6000]
    
    async def _call_llm_api(self, messages: list, max_retries: int = 3) -> Optional[str]:
        """调用LLM API"""
        if not self.api_key:
            print(f"[ERROR] {self.provider} API key not configured")
            return None
            
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens
        }
        
        if self.provider == "zhipu":
            data["stream"] = False
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.post(
                        f"{self.base_url}/chat/completions",
                        headers=headers,
                        json=data
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        content = result["choices"][0]["message"]["content"]
                        return content
                    else:
                        print(f"⚠️  API错误: {response.status_code}")
                        
            except Exception as e:
                print(f"⚠️  API调用异常 (尝试 {attempt + 1}/{max_retries}): {e}")
                
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
        
        return None
    
    async def extract_for_notion_enhanced(self, html: str, url: str, job_data: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
        """增强版Notion提取方法"""
        if not html or not html.strip():
            return None
        
        print(f"🔄 开始增强提取: {url}")
        
        # 1. 结构化提取
        structured_info = self._extract_structured_info(html, url, job_data)
        
        # 2. LLM提取岗位描述和公司名称，以及补充招募方向
        processed_html = self._prepare_html_for_llm(html)
        
        prompt = f"""
请从以下招聘页面内容中提取信息。注意：只提取招聘岗位的信息，不要提取HR的个人信息。

页面内容：
{processed_html}

请提取以下信息，以JSON格式返回：

1. **岗位描述**：详细的工作职责和技能要求，包括：
   - 具体的工作内容和职责
   - 技能要求和技术栈
   - 任职要求和条件
   注意：只要核心岗位内容，去除公司介绍、福利待遇、联系方式等
   
2. **公司名称**：招聘公司的准确名称

3. **发布日期**：如果页面中明确显示岗位发布时间，提取格式为YYYY-MM-DD，没有则为空字符串
   重要提醒：
   - 只要真正的岗位发布日期，不要公司成立日期
   - 不要HR注册时间、公司创建时间、更新时间
   - 不要任何非岗位相关的日期
   - 如果不确定是否为岗位发布日期，请设为空字符串
   
4. **发布日期来源**：说明你从页面的哪个部分提取到发布日期，必须明确是岗位发布相关，如果没有找到真正的岗位发布日期则为空字符串

5. **招募方向**：如果页面中提到具体的技术方向或招募方向，请提取出来。如预训练方向、大数据方向、创新方向、多模态方向等。没有则为空字符串。

要求：
- 专注于招聘岗位的核心信息
- 岗位描述要完整但简洁，突出关键职责和技能
- 对发布日期要特别谨慎，宁可为空也不要错误的日期
- 如果字段不存在则设为空字符串
- 只返回JSON格式，不要其他文字

返回格式：
{{
  "岗位描述": "详细的岗位职责和技能要求...",
  "公司名称": "公司名称", 
  "发布日期": "YYYY-MM-DD或空字符串",
  "发布日期来源": "明确说明从页面哪里提取到岗位发布日期，没有则为空",
  "招募方向": "具体的技术方向或招募方向，没有则为空"
}}
"""
        
        messages = [{"role": "user", "content": prompt}]
        
        try:
            content = await self._call_llm_api(messages)
            
            if content:
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group()
                    llm_data = json.loads(json_str)
                    
                    # 合并结构化提取和LLM提取的结果
                    raw_graduation_req = structured_info.get("毕业时间要求", "")
                    raw_deadline = structured_info.get("招聘截止日期", "")
                    
                    # 日期标准化
                    standardized_graduation = self.standardize_date_format(raw_graduation_req) if raw_graduation_req else ""
                    standardized_deadline = self.standardize_date_format(raw_deadline) if raw_deadline else ""
                    
                    # 招募方向合并（优先使用LLM提取的，如果为空则使用正则提取的）
                    recruitment_direction = llm_data.get("招募方向", "") or structured_info.get("招募方向", "")
                    
                    final_result = {
                        "岗位名称": structured_info.get("岗位名称", ""),
                        "岗位描述": llm_data.get("岗位描述", ""),
                        "发布日期": llm_data.get("发布日期", ""),
                        "发布日期来源": llm_data.get("发布日期来源", ""),
                        "发布平台": structured_info.get("发布平台", ""),
                        "HR活跃度": structured_info.get("HR活跃度", ""),
                        "公司名称": llm_data.get("公司名称", ""),
                        "薪资": structured_info.get("薪资", ""),
                        "经验要求": structured_info.get("经验要求", ""),
                        "工作地点": structured_info.get("工作地点", ""),
                        "岗位链接": url,
                        "页面抓取时间": structured_info.get("页面抓取时间", ""),
                        
                        # 新增字段
                        "毕业时间要求": raw_graduation_req,
                        "毕业时间要求_标准化": standardized_graduation,
                        "毕业时间_匹配状态": self.check_graduation_eligibility(raw_graduation_req),
                        "招聘截止日期": raw_deadline,
                        "招聘截止日期_标准化": standardized_deadline,
                        "招聘截止日期_状态": self.check_deadline_status(standardized_deadline),
                        "招募方向": recruitment_direction,
                        
                        "提取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    # 调试信息
                    print(f"[OK] Enhanced extraction completed:")
                    print(f"   岗位名称: {final_result.get('岗位名称', 'N/A')}")
                    print(f"   薪资: {final_result.get('薪资', 'N/A')}")
                    print(f"   地点: {final_result.get('工作地点', 'N/A')}")
                    print(f"   经验: {final_result.get('经验要求', 'N/A')}")
                    print(f"   🎓 毕业时间要求: {final_result.get('毕业时间要求', 'N/A')}")
                    print(f"   📊 匹配状态: {final_result.get('毕业时间_匹配状态', 'N/A')}")
                    print(f"   ⏰ 招聘截止日期: {final_result.get('招聘截止日期', 'N/A')} -> {final_result.get('招聘截止日期_标准化', 'N/A')}")
                    print(f"   📈 截止状态: {final_result.get('招聘截止日期_状态', 'N/A')}")
                    print(f"   🎯 招募方向: {final_result.get('招募方向', 'N/A')}")
                    
                    return final_result
                    
        except Exception as e:
            print(f"⚠️  LLM提取失败: {e}")
        
        # 如果LLM失败，返回结构化提取的结果
        raw_graduation_req = structured_info.get("毕业时间要求", "")
        raw_deadline = structured_info.get("招聘截止日期", "")
        standardized_graduation = self.standardize_date_format(raw_graduation_req) if raw_graduation_req else ""
        standardized_deadline = self.standardize_date_format(raw_deadline) if raw_deadline else ""
        
        fallback_result = {
            "岗位名称": structured_info.get("岗位名称", ""),
            "岗位描述": "暂无详细描述",
            "发布日期": "",
            "发布日期来源": "",
            "发布平台": structured_info.get("发布平台", ""),
            "HR活跃度": structured_info.get("HR活跃度", ""),
            "公司名称": "",
            "薪资": structured_info.get("薪资", ""),
            "经验要求": structured_info.get("经验要求", ""),
            "工作地点": structured_info.get("工作地点", ""),
            "岗位链接": url,
            "页面抓取时间": structured_info.get("页面抓取时间", ""),
            
            # 新增字段（仅结构化提取）
            "毕业时间要求": raw_graduation_req,
            "毕业时间要求_标准化": standardized_graduation,
            "毕业时间_匹配状态": self.check_graduation_eligibility(raw_graduation_req),
            "招聘截止日期": raw_deadline,
            "招聘截止日期_标准化": standardized_deadline,
            "招聘截止日期_状态": self.check_deadline_status(standardized_deadline),
            "招募方向": structured_info.get("招募方向", ""),
            
            "提取时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return fallback_result

# 测试增强版提取器
async def test_enhanced_extractor():
    """测试增强版提取器"""
    import glob
    
    print("🧪 测试增强版Notion提取器")
    print("=" * 80)
    
    # 查找数据文件
    data_patterns = ["../../data/raw_boss_playwright_*.jsonl", "data/raw_boss_playwright_*.jsonl"]
    data_files = []
    for pattern in data_patterns:
        data_files.extend(glob.glob(pattern, recursive=True))
    
    if not data_files:
        print("[ERROR] No data files found")
        return
    
    data_file = max(data_files, key=os.path.getmtime)
    print(f"📁 使用数据文件: {data_file}")
    
    extractor = EnhancedNotionExtractor()
    results = []
    
    with open(data_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 3:  # 测试前3个
                break
            
            try:
                job_data = json.loads(line.strip())
                html = job_data.get('html', '')
                url = job_data.get('url', '')
                
                if html:
                    print(f"\n{'='*20} 测试岗位 {i+1}/3 {'='*20}")
                    result = await extractor.extract_for_notion_enhanced(html, url, job_data)
                    
                    if result:
                        results.append(result)
                    
                    await asyncio.sleep(2)
                    
            except Exception as e:
                print(f"⚠️  处理失败: {e}")
    
    # 保存结果
    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"enhanced_notion_jobs_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 增强结果已保存: {output_file}")
        print(f"📊 新增字段统计:")
        
        new_fields = ["毕业时间要求", "毕业时间_匹配状态", "招聘截止日期", "招聘截止日期_状态", "招募方向"]
        for field in new_fields:
            non_empty = sum(1 for job in results if job.get(field, '').strip())
            print(f"   {field}: {non_empty}/{len(results)}")
        
        # 显示匹配状态统计
        match_statuses = {}
        for job in results:
            status = job.get('毕业时间_匹配状态', '未知')
            match_statuses[status] = match_statuses.get(status, 0) + 1
        
        print(f"\n🎯 毕业时间匹配状态分布:")
        for status, count in match_statuses.items():
            print(f"   {status}: {count}个")

if __name__ == "__main__":
    asyncio.run(test_enhanced_extractor())