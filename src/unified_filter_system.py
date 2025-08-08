# unified_filter_system.py - 统一的分层筛选系统
"""
合并job_filter_system和optimized_filter_system，提供完整的筛选解决方案：
1. 基础筛选：硬性条件过滤（薪资、地点、经验、毕业时间、截止日期）
2. 高级筛选：智能评分排序（公司知名度、业务领域匹配）
3. 统一配置管理
4. 简化Notion字段结构
"""
import re
import json
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta

class FilterType(Enum):
    BASIC = "basic"      # 基础筛选：硬性条件
    ADVANCED = "advanced"  # 高级筛选：智能评分

@dataclass
class FilterResult:
    """筛选结果"""
    score: float  # 0-1分数
    reason: str   # 筛选原因
    details: Dict[str, Any]  # 详细信息

class BaseFilter(ABC):
    """筛选器基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.enabled = config.get('enabled', True)
        self.weight = config.get('weight', 1.0)
        self.threshold = config.get('threshold', 0.0)
        self.is_hard_filter = config.get('is_hard_filter', False)  # 是否为硬性筛选
    
    @abstractmethod
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        """执行筛选"""
        pass
    
    @abstractmethod
    def get_filter_type(self) -> FilterType:
        """获取筛选类型"""
        pass

# ========== 基础筛选器（硬性条件） ==========

class SalaryFilter(BaseFilter):
    """薪资筛选器 - 支持硬性筛选"""
    
    def get_filter_type(self) -> FilterType:
        return FilterType.BASIC
    
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        salary_text = job.get('薪资', '') or self._extract_from_description(job)
        
        if not salary_text:
            return FilterResult(0.5, "无薪资信息", {"salary_range": None})
        
        salary_range = self._parse_salary(salary_text)
        if not salary_range:
            return FilterResult(0.5, "薪资解析失败", {"salary_text": salary_text})
        
        min_salary, max_salary = salary_range
        
        # 硬性筛选阈值
        hard_min = self.config.get('hard_min_salary', 15)  # 最低接受薪资
        hard_max = self.config.get('hard_max_salary', 80)  # 最高接受薪资
        
        # 硬性筛选
        if max_salary < hard_min:
            return FilterResult(0.0, f"薪资过低({max_salary}k < {hard_min}k)", 
                              {"salary_range": salary_range, "reject_reason": "below_minimum"})
        
        if min_salary > hard_max:
            return FilterResult(0.0, f"薪资过高({min_salary}k > {hard_max}k)", 
                              {"salary_range": salary_range, "reject_reason": "above_maximum"})
        
        # 软性评分
        target_salary = self.config.get('target_salary', 30)
        mid_salary = (min_salary + max_salary) / 2
        score = self._calculate_salary_score(mid_salary, target_salary)
        
        # 生成建议
        suggestion = self._generate_salary_suggestion(mid_salary, target_salary)
        
        return FilterResult(score, f"薪资匹配({min_salary}-{max_salary}k)", {
            "salary_range": salary_range,
            "mid_salary": mid_salary,
            "target_salary": target_salary,
            "suggestion": suggestion
        })
    
    def _parse_salary(self, salary_text: str) -> Optional[Tuple[int, int]]:
        """解析薪资范围"""
        patterns = [
            (r'(\d+)[-~到](\d+)[kK](?:·\d+薪)?', 1),  # 20-30k·13薪
            (r'(\d+)[-~到](\d+)万(?:·\d+薪)?', 10),    # 2-3万·13薪
            (r'(\d+)[kK][-~到](\d+)[kK]', 1),         # 20k-30k
            (r'(\d+)\+[kK]', lambda x: (x, x * 1.3)), # 25k+
            (r'(\d+)万\+', lambda x: (x * 10, x * 13)), # 3万+
            (r'(\d+)[kK](?:·\d+薪)?$', lambda x: (x * 0.9, x * 1.1)), # 30k·13薪
        ]
        
        for pattern, multiplier in patterns:
            match = re.search(pattern, salary_text)
            if match:
                if callable(multiplier):
                    return multiplier(int(match.group(1)))
                else:
                    min_val = int(match.group(1)) * multiplier
                    max_val = int(match.group(2)) * multiplier
                    return (min_val, max_val)
        
        return None
    
    def _calculate_salary_score(self, mid_salary: float, target_salary: float) -> float:
        """计算薪资评分"""
        ratio = mid_salary / target_salary
        
        if 0.9 <= ratio <= 1.1:
            return 1.0  # 完美匹配
        elif 0.8 <= ratio < 0.9 or 1.1 < ratio <= 1.3:
            return 0.8  # 良好匹配
        elif 0.7 <= ratio < 0.8 or 1.3 < ratio <= 1.5:
            return 0.6  # 可接受
        else:
            return 0.3  # 偏差较大
    
    def _generate_salary_suggestion(self, mid_salary: float, target_salary: float) -> str:
        """生成薪资建议"""
        ratio = mid_salary / target_salary
        
        if ratio >= 1.2:
            return f"薪资高于预期{(ratio-1)*100:.0f}%，优秀机会"
        elif ratio >= 1.0:
            return f"薪资符合预期，推荐申请"
        elif ratio >= 0.8:
            return f"薪资略低于预期，可考虑其他优势"
        else:
            return f"薪资偏低{(1-ratio)*100:.0f}%，谨慎考虑"
    
    def _extract_from_description(self, job: Dict[str, Any]) -> str:
        """从岗位描述提取薪资"""
        description = job.get('岗位描述', '')
        patterns = [r'\d+[-~到]\d+[kK万]', r'薪资[:：]\s*\d+']
        for pattern in patterns:
            match = re.search(pattern, description)
            if match:
                return match.group(0)
        return ''

class LocationFilter(BaseFilter):
    """工作地点筛选器 - 支持硬性筛选"""
    
    def get_filter_type(self) -> FilterType:
        return FilterType.BASIC
    
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        location = job.get('工作地点', '').strip()
        if not location:
            return FilterResult(0.5, "无地点信息", {})
        
        preferred_cities = self.config.get('preferred_cities', ['北京', '上海', '深圳'])
        acceptable_cities = self.config.get('acceptable_cities', ['杭州', '广州', '成都'])
        rejected_cities = self.config.get('rejected_cities', [])
        remote_keywords = ['远程', '在家办公', 'remote', 'work from home']
        
        location_lower = location.lower()
        
        # 硬性拒绝城市
        for city in rejected_cities:
            if city in location:
                return FilterResult(0.0, f"拒绝城市({city})", {
                    "matched_city": city,
                    "reject_reason": "blacklisted_city"
                })
        
        # 远程工作
        if any(keyword in location_lower for keyword in remote_keywords):
            return FilterResult(1.0, "远程工作", {
                "location_type": "remote",
                "suggestion": "远程工作，无地理限制"
            })
        
        # 首选城市
        for city in preferred_cities:
            if city in location:
                return FilterResult(1.0, f"首选城市({city})", {
                    "matched_city": city,
                    "location_type": "preferred",
                    "suggestion": f"{city}是首选工作城市"
                })
        
        # 可接受城市
        for city in acceptable_cities:
            if city in location:
                return FilterResult(0.8, f"可接受城市({city})", {
                    "matched_city": city,
                    "location_type": "acceptable",
                    "suggestion": f"{city}可以考虑"
                })
        
        return FilterResult(0.4, f"其他城市({location})", {
            "location": location,
            "location_type": "other",
            "suggestion": "需要评估搬迁成本"
        })

class ExperienceFilter(BaseFilter):
    """经验要求筛选器"""
    
    def get_filter_type(self) -> FilterType:
        return FilterType.BASIC
    
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        experience_text = job.get('经验要求', '')
        if not experience_text:
            return FilterResult(0.7, "无经验要求", {"suggestion": "经验要求不明确，可尝试申请"})
        
        user_experience = self.config.get('user_experience_years', 1.0)
        parsed = self._parse_experience(experience_text)
        
        if not parsed['parsed_successfully']:
            return FilterResult(0.5, "经验要求解析失败", {
                **parsed,
                "suggestion": "需要人工分析经验要求"
            })
        
        score = self._calculate_experience_score(user_experience, parsed)
        reason = self._generate_experience_reason(user_experience, parsed, score)
        suggestion = self._generate_experience_suggestion(user_experience, parsed, score)
        
        return FilterResult(score, reason, {
            **parsed,
            "user_experience": user_experience,
            "suggestion": suggestion
        })
    
    def _parse_experience(self, text: str) -> Dict[str, Any]:
        """解析经验要求"""
        text_lower = text.lower()
        
        # 应届生/实习生
        if any(keyword in text_lower for keyword in ['应届', '实习', '校招', '毕业生']):
            return {
                "min_years": 0,
                "max_years": 0,
                "is_fresh_grad": True,
                "parsed_successfully": True,
                "requirement_type": "应届生"
            }
        
        # 经验不限
        if any(keyword in text_lower for keyword in ['不限', '无要求', '经验不限']):
            return {
                "min_years": 0,
                "max_years": None,
                "is_unlimited": True,
                "parsed_successfully": True,
                "requirement_type": "经验不限"
            }
        
        # 解析具体年限
        patterns = [
            (r'(\d+)[-~到](\d+)年', 'range'),
            (r'(\d+)年以上', 'min_plus'),
            (r'(\d+)\+年', 'min_plus'),
            (r'(\d+)年', 'exact'),
        ]
        
        for pattern, pattern_type in patterns:
            match = re.search(pattern, text)
            if match:
                if pattern_type == 'range':
                    min_years = float(match.group(1))
                    max_years = float(match.group(2))
                elif pattern_type == 'min_plus':
                    min_years = float(match.group(1))
                    max_years = None
                else:  # exact
                    exact_years = float(match.group(1))
                    min_years = max(0, exact_years - 0.5)
                    max_years = exact_years + 0.5
                
                return {
                    "min_years": min_years,
                    "max_years": max_years,
                    "parsed_successfully": True,
                    "requirement_type": f"{pattern_type}_{min_years}"
                }
        
        return {"parsed_successfully": False}
    
    def _calculate_experience_score(self, user_exp: float, parsed: Dict) -> float:
        """计算经验匹配分数"""
        if parsed.get('is_fresh_grad'):
            return 0.9 if user_exp <= 2 else 0.5
        
        if parsed.get('is_unlimited'):
            return 0.8
        
        min_req = parsed.get('min_years', 0)
        max_req = parsed.get('max_years')
        
        if max_req is None:  # X年以上
            if user_exp >= min_req:
                if user_exp <= min_req + 2:
                    return 1.0  # 完美匹配
                else:
                    return 0.8  # 过度匹配但可接受
            else:
                gap = min_req - user_exp
                return 0.6 if gap <= 1 else 0.2
        else:  # X-Y年
            if min_req <= user_exp <= max_req:
                return 1.0  # 完美匹配
            elif user_exp < min_req:
                gap = min_req - user_exp
                return 0.6 if gap <= 1 else 0.2
            else:
                return 0.8  # 过度匹配
    
    def _generate_experience_reason(self, user_exp: float, parsed: Dict, score: float) -> str:
        """生成经验匹配原因"""
        if score >= 0.9:
            return f"经验完全匹配({user_exp}年)"
        elif score >= 0.6:
            return f"经验基本匹配({user_exp}年)"
        else:
            return f"经验不匹配({user_exp}年)"
    
    def _generate_experience_suggestion(self, user_exp: float, parsed: Dict, score: float) -> str:
        """生成经验建议"""
        if parsed.get('is_fresh_grad'):
            return "面向应届生，适合申请"
        
        min_req = parsed.get('min_years', 0)
        max_req = parsed.get('max_years')
        
        if score >= 0.9:
            return "经验完全符合要求，强烈推荐申请"
        elif score >= 0.6:
            return "经验基本符合，可以申请"
        else:
            if user_exp < min_req:
                gap = min_req - user_exp
                return f"经验不足{gap:.1f}年，可通过项目经验补充"
            else:
                return "经验过于丰富，可能存在过度匹配"

class GraduationFilter(BaseFilter):
    """毕业时间筛选器 - 硬性筛选"""
    
    def get_filter_type(self) -> FilterType:
        return FilterType.BASIC
    
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        graduation_req = job.get('毕业时间要求', '')
        if not graduation_req:
            return FilterResult(0.8, "无毕业时间要求", {"suggestion": "无明确毕业时间要求"})
        
        user_graduation = self.config.get('user_graduation', '2023-12')
        standardized = self._standardize_graduation(graduation_req)
        
        # 硬性筛选逻辑
        if '2024届' in graduation_req and '2023-12' in user_graduation:
            return FilterResult(1.0, "2024届完全匹配", {
                "requirement": standardized,
                "match_status": "完全符合",
                "suggestion": "毕业时间完全符合要求"
            })
        elif '2025届' in graduation_req:
            return FilterResult(0.0, "2025届不匹配", {
                "requirement": standardized,
                "match_status": "不符合",
                "suggestion": "2025届为2024年毕业，不符合条件",
                "reject_reason": "graduation_mismatch"
            })
        elif '2023届' in graduation_req:
            return FilterResult(0.0, "2023届已过期", {
                "requirement": standardized,
                "match_status": "已过期",
                "suggestion": "2023届招聘已结束",
                "reject_reason": "expired_graduation"
            })
        elif any(keyword in graduation_req for keyword in ['应届', '校招']):
            return FilterResult(0.8, "应届生要求", {
                "requirement": standardized,
                "match_status": "需要确认",
                "suggestion": "应届生招聘，建议查看详细要求"
            })
        else:
            return FilterResult(0.5, "毕业时间需确认", {
                "requirement": standardized,
                "match_status": "需要确认",
                "suggestion": "毕业时间要求不明确，需要人工确认"
            })
    
    def _standardize_graduation(self, graduation_req: str) -> str:
        """标准化毕业时间要求"""
        if not graduation_req:
            return ""
        
        # 届别格式保持不变
        if '届' in graduation_req:
            return graduation_req
        
        # 其他格式简单处理
        return graduation_req.strip()

class DeadlineFilter(BaseFilter):
    """招聘截止日期筛选器 - 硬性筛选"""
    
    def get_filter_type(self) -> FilterType:
        return FilterType.BASIC
    
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        deadline_text = job.get('招聘截止日期', '')
        if not deadline_text:
            return FilterResult(0.8, "无截止日期", {"suggestion": "无明确截止日期，建议尽快申请"})
        
        # 标准化截止日期
        standardized_date = self._standardize_date(deadline_text)
        if not standardized_date:
            return FilterResult(0.5, "截止日期解析失败", {
                "original": deadline_text,
                "suggestion": "日期格式异常，需要人工确认"
            })
        
        # 保存标准化日期到job对象
        job['招聘截止日期_标准化'] = standardized_date
        
        # 检查是否过期
        try:
            deadline = datetime.strptime(standardized_date, "%Y-%m-%d")
            now = datetime.now()
            days_left = (deadline - now).days
            
            if deadline < now:
                return FilterResult(0.0, "招聘已截止", {
                    "deadline": standardized_date,
                    "days_left": days_left,
                    "status": "已过期",
                    "suggestion": "招聘已结束",
                    "reject_reason": "expired_deadline"
                })
            elif days_left <= 3:
                return FilterResult(0.7, f"即将截止({days_left}天)", {
                    "deadline": standardized_date,
                    "days_left": days_left,
                    "status": "即将截止",
                    "suggestion": f"还有{days_left}天截止，需要尽快申请"
                })
            elif days_left <= 7:
                return FilterResult(0.9, f"近期截止({days_left}天)", {
                    "deadline": standardized_date,
                    "days_left": days_left,
                    "status": "近期截止",
                    "suggestion": f"还有{days_left}天截止，建议优先申请"
                })
            else:
                return FilterResult(1.0, f"充裕时间({days_left}天)", {
                    "deadline": standardized_date,
                    "days_left": days_left,
                    "status": "未过期",
                    "suggestion": f"还有{days_left}天时间准备申请"
                })
                
        except ValueError:
            return FilterResult(0.5, "日期格式错误", {
                "deadline": standardized_date,
                "suggestion": "日期格式无法解析，需要人工确认"
            })
    
    def _standardize_date(self, date_str: str) -> Optional[str]:
        """标准化日期格式为YYYY-MM-DD"""
        if not date_str:
            return None
        
        # 移除前缀
        text = re.sub(r'^(截止日期|报名截止|申请截止|招聘截止)[：:]\s*', '', date_str.strip())
        
        patterns = [
            (r'(\d{4})[./年](\d{1,2})[./月](\d{1,2})[日]?', 
             lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
            (r'(\d{4})[./年](\d{1,2})[月]?', 
             lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-01"),
            (r'(\d{4})年?', 
             lambda m: f"{m.group(1)}-01-01"),
            # 处理简化年份格式
            (r'(\d{2})[./年](\d{1,2})[./月](\d{1,2})[日]?', 
             lambda m: f"20{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
        ]
        
        for pattern, formatter in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return formatter(match)
                except:
                    continue
        
        return None

# ========== 高级筛选器（智能评分） ==========

class CompanyFameFilter(BaseFilter):
    """公司知名度筛选器"""
    
    def get_filter_type(self) -> FilterType:
        return FilterType.ADVANCED
    
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        company = job.get('公司名称', '').strip()
        if not company:
            return FilterResult(0.3, "无公司信息", {})
        
        # 知名公司分类
        tier1_companies = self.config.get('tier1_companies', [
            '华为', '腾讯', '字节跳动', '阿里巴巴', '百度', '美团', '滴滴', '小米', '京东'
        ])
        
        tier2_companies = self.config.get('tier2_companies', [
            '商汤', '旷视', '依图', '第四范式', '寒武纪', '地平线', '云从', '澎思'
        ])
        
        tier3_companies = self.config.get('tier3_companies', [
            '网易', '搜狗', '360', '新浪', '搜狐', '爱奇艺', '优酷', '携程'
        ])
        
        # 匹配逻辑
        for company_name in tier1_companies:
            if company_name in company:
                return FilterResult(1.0, f"顶级大厂({company_name})", {
                    "company_tier": "tier1",
                    "matched_company": company_name,
                    "suggestion": "顶级互联网公司，强烈推荐"
                })
        
        for company_name in tier2_companies:
            if company_name in company:
                return FilterResult(0.9, f"知名AI公司({company_name})", {
                    "company_tier": "tier2",
                    "matched_company": company_name,
                    "suggestion": "知名AI独角兽，技术实力强"
                })
        
        for company_name in tier3_companies:
            if company_name in company:
                return FilterResult(0.8, f"知名公司({company_name})", {
                    "company_tier": "tier3",
                    "matched_company": company_name,
                    "suggestion": "知名互联网公司，可以考虑"
                })
        
        # 检查是否为大公司（通过关键词）
        big_company_keywords = ['科技', '集团', '股份', '有限公司']
        if any(keyword in company for keyword in big_company_keywords):
            return FilterResult(0.5, f"普通企业({company})", {
                "company_tier": "normal",
                "suggestion": "需要进一步了解公司背景"
            })
        
        return FilterResult(0.3, f"小型公司({company})", {
            "company_tier": "small",
            "suggestion": "小型公司，需要谨慎评估发展前景"
        })

class BusinessDomainFilter(BaseFilter):
    """业务领域筛选器"""
    
    def get_filter_type(self) -> FilterType:
        return FilterType.ADVANCED
    
    def filter(self, job: Dict[str, Any]) -> FilterResult:
        # 分析文本
        job_description = job.get('岗位描述', '')
        job_title = job.get('岗位名称', '')
        recruitment_direction = job.get('招募方向', '')
        
        text_to_analyze = f"{job_title} {job_description} {recruitment_direction}"
        
        if not text_to_analyze.strip():
            return FilterResult(0.5, "无业务描述", {"suggestion": "缺少详细业务描述"})
        
        # 业务领域关键词
        core_domains = self.config.get('core_domains', [
            '大模型', 'LLM', 'GPT', 'ChatGPT', 'Claude', '语言模型'
        ])
        
        ai_domains = self.config.get('ai_domains', [
            '机器学习', '深度学习', '人工智能', 'AI', '神经网络', '自然语言处理', 'NLP'
        ])
        
        related_domains = self.config.get('related_domains', [
            '算法', '数据科学', '推荐系统', '计算机视觉', '语音识别', '知识图谱'
        ])
        
        text_lower = text_to_analyze.lower()
        matched_domains = []
        
        # 核心领域匹配
        for domain in core_domains:
            if domain.lower() in text_lower:
                matched_domains.append(("core", domain))
        
        # AI领域匹配
        for domain in ai_domains:
            if domain.lower() in text_lower:
                matched_domains.append(("ai", domain))
        
        # 相关领域匹配
        for domain in related_domains:
            if domain.lower() in text_lower:
                matched_domains.append(("related", domain))
        
        if not matched_domains:
            return FilterResult(0.3, "无匹配领域", {
                "suggestion": "业务领域与个人方向不太匹配"
            })
        
        # 计算匹配分数
        core_matches = [d for t, d in matched_domains if t == "core"]
        ai_matches = [d for t, d in matched_domains if t == "ai"]
        related_matches = [d for t, d in matched_domains if t == "related"]
        
        if core_matches:
            score = 1.0
            reason = f"核心领域匹配({', '.join(core_matches[:2])})"
            suggestion = "与核心技术方向高度匹配，强烈推荐"
        elif ai_matches:
            score = 0.8
            reason = f"AI领域匹配({', '.join(ai_matches[:2])})"
            suggestion = "与AI技术方向匹配，推荐申请"
        elif related_matches:
            score = 0.6
            reason = f"相关领域匹配({', '.join(related_matches[:2])})"
            suggestion = "与相关技术领域匹配，可以考虑"
        else:
            score = 0.3
            reason = "无明显匹配"
            suggestion = "技术领域匹配度不高"
        
        return FilterResult(score, reason, {
            "matched_domains": matched_domains,
            "core_matches": core_matches,
            "ai_matches": ai_matches,
            "related_matches": related_matches,
            "suggestion": suggestion
        })

# ========== 筛选管理器 ==========

class UnifiedJobFilterManager:
    """统一的岗位筛选管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.basic_filters = self._init_basic_filters()
        self.advanced_filters = self._init_advanced_filters()
        
        print(f"✅ 统一筛选器初始化完成: {len(self.basic_filters)}个基础筛选器, {len(self.advanced_filters)}个高级筛选器")
    
    def _init_basic_filters(self) -> Dict[str, BaseFilter]:
        """初始化基础筛选器"""
        basic_config = self.config.get('basic', {})
        enabled_filters = basic_config.get('enabled', 
            ['salary', 'location', 'experience', 'graduation', 'deadline'])
        
        filters = {}
        filter_classes = {
            'salary': SalaryFilter,
            'location': LocationFilter,
            'experience': ExperienceFilter,
            'graduation': GraduationFilter,
            'deadline': DeadlineFilter
        }
        
        for filter_name in enabled_filters:
            if filter_name in filter_classes:
                filter_config = basic_config.get(filter_name, {})
                # 合并公共配置
                filter_config.update(basic_config.get('common', {}))
                filters[filter_name] = filter_classes[filter_name](filter_config)
        
        return filters
    
    def _init_advanced_filters(self) -> Dict[str, BaseFilter]:
        """初始化高级筛选器"""
        advanced_config = self.config.get('advanced', {})
        enabled_filters = advanced_config.get('enabled', ['company_fame', 'business_domain'])
        
        filters = {}
        filter_classes = {
            'company_fame': CompanyFameFilter,
            'business_domain': BusinessDomainFilter
        }
        
        for filter_name in enabled_filters:
            if filter_name in filter_classes:
                filter_config = advanced_config.get(filter_name, {})
                filters[filter_name] = filter_classes[filter_name](filter_config)
        
        return filters
    
    def apply_basic_filters(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """应用基础筛选器 - 硬性筛选"""
        if not jobs:
            return []
        
        print(f"🔍 开始基础筛选: {len(jobs)}个岗位")
        
        filtered_jobs = []
        hard_filter_count = 0
        basic_config = self.config.get('basic', {})
        global_threshold = basic_config.get('global_threshold', 0.3)
        
        for job in jobs:
            scores = {}
            total_score = 0
            total_weight = 0
            hard_filter_failed = False
            
            # 应用所有基础筛选器
            for filter_name, filter_obj in self.basic_filters.items():
                if not filter_obj.enabled:
                    continue
                
                result = filter_obj.filter(job)
                scores[filter_name] = result
                
                # 硬性筛选检查 - score为0表示硬性拒绝
                if result.score == 0.0:
                    hard_filter_failed = True
                    hard_filter_count += 1
                    print(f"   ❌ 硬性筛选拒绝: {job.get('岗位名称', 'N/A')} - {result.reason}")
                    break
                
                # 加权计算
                total_score += result.score * filter_obj.weight
                total_weight += filter_obj.weight
            
            # 硬性筛选未通过，直接跳过
            if hard_filter_failed:
                continue
            
            # 计算综合分数
            final_score = total_score / total_weight if total_weight > 0 else 0
            
            # 软性筛选阈值检查
            if final_score >= global_threshold:
                job['_basic_filter_score'] = final_score
                job['_basic_filter_details'] = scores
                filtered_jobs.append(job)
                print(f"   ✅ 基础筛选通过: {job.get('岗位名称', 'N/A')} (分数: {final_score:.2f})")
            else:
                print(f"   ⚠️  分数过低: {job.get('岗位名称', 'N/A')} (分数: {final_score:.2f} < {global_threshold})")
        
        print(f"✅ 基础筛选完成: {len(filtered_jobs)}/{len(jobs)}个岗位通过")
        print(f"   硬性筛选过滤: {hard_filter_count}个岗位")
        print(f"   软性筛选过滤: {len(jobs) - len(filtered_jobs) - hard_filter_count}个岗位")
        
        return filtered_jobs
    
    def apply_advanced_filters(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """应用高级筛选器 - 智能评分排序"""
        if not jobs:
            return []
        
        print(f"🔍 开始高级筛选: {len(jobs)}个岗位")
        
        for job in jobs:
            scores = {}
            total_score = 0
            total_weight = 0
            
            # 应用所有高级筛选器
            for filter_name, filter_obj in self.advanced_filters.items():
                if not filter_obj.enabled:
                    continue
                
                result = filter_obj.filter(job)
                scores[filter_name] = result
                
                # 加权计算
                total_score += result.score * filter_obj.weight
                total_weight += filter_obj.weight
            
            # 计算高级筛选分数
            advanced_score = total_score / total_weight if total_weight > 0 else 0.5
            
            # 合并基础和高级分数
            basic_score = job.get('_basic_filter_score', 0.5)
            basic_weight = self.config.get('score_weights', {}).get('basic', 0.6)
            advanced_weight = self.config.get('score_weights', {}).get('advanced', 0.4)
            
            final_score = basic_score * basic_weight + advanced_score * advanced_weight
            
            # 保存评分信息
            job['综合评分'] = round(final_score * 100)  # 转为0-100分
            job['推荐等级'] = self._get_recommendation_level(final_score)
            job['_advanced_filter_details'] = scores
            job['_final_score'] = final_score
            
            # 生成匹配建议
            job['匹配建议'] = self._generate_match_suggestion(job, scores)
        
        # 按分数排序
        jobs.sort(key=lambda x: x.get('_final_score', 0), reverse=True)
        
        print(f"✅ 高级筛选完成: {len(jobs)}个岗位已评分排序")
        self._print_score_distribution(jobs)
        
        return jobs
    
    def _get_recommendation_level(self, score: float) -> str:
        """获取推荐等级"""
        if score >= 0.8:
            return "🌟 强烈推荐"
        elif score >= 0.65:
            return "✨ 推荐"
        elif score >= 0.5:
            return "⚠️ 可考虑"
        else:
            return "❌ 不推荐"
    
    def _generate_match_suggestion(self, job: Dict[str, Any], advanced_scores: Dict) -> str:
        """生成匹配建议"""
        suggestions = []
        
        # 从基础筛选获取建议
        basic_details = job.get('_basic_filter_details', {})
        for filter_name, result in basic_details.items():
            if result.details.get('suggestion'):
                suggestions.append(result.details['suggestion'])
        
        # 从高级筛选获取建议
        for filter_name, result in advanced_scores.items():
            if result.details.get('suggestion'):
                suggestions.append(result.details['suggestion'])
        
        # 综合建议
        final_score = job.get('_final_score', 0)
        if final_score >= 0.8:
            suggestions.insert(0, "综合匹配度很高，强烈建议申请")
        elif final_score >= 0.65:
            suggestions.insert(0, "整体匹配较好，建议申请")
        elif final_score >= 0.5:
            suggestions.insert(0, "有一定匹配度，可以考虑")
        else:
            suggestions.insert(0, "匹配度不高，需要谨慎考虑")
        
        return " | ".join(suggestions[:3])  # 限制建议数量
    
    def _print_score_distribution(self, jobs: List[Dict[str, Any]]):
        """打印分数分布"""
        if not jobs:
            return
        
        levels = {}
        for job in jobs:
            level = job.get('推荐等级', 'Unknown')
            levels[level] = levels.get(level, 0) + 1
        
        print(f"📊 推荐等级分布:")
        for level, count in levels.items():
            print(f"   {level}: {count}个岗位")

# ========== 配置管理 ==========

def get_unified_filter_config() -> Dict[str, Any]:
    """获取统一的筛选配置"""
    return {
        "basic": {
            "enabled": ["salary", "location", "experience", "graduation", "deadline"],
            "global_threshold": 0.3,  # 基础筛选全局阈值
            
            # 公共配置
            "common": {
                "user_experience_years": 1.0,  # 用户工作经验年限
                "user_graduation": "2023-12"   # 用户毕业时间
            },
            
            # 薪资筛选配置
            "salary": {
                "hard_min_salary": 15,    # 硬性最低薪资
                "hard_max_salary": 80,    # 硬性最高薪资
                "target_salary": 30,      # 目标薪资
                "weight": 0.3,
                "is_hard_filter": False   # 不启用硬性筛选
            },
            
            # 地点筛选配置
            "location": {
                "preferred_cities": ["北京", "上海", "深圳"],
                "acceptable_cities": ["杭州", "广州", "成都", "武汉", "西安", "南京"],
                "rejected_cities": [],     # 拒绝的城市
                "weight": 0.2,
                "is_hard_filter": True    # 启用硬性筛选
            },
            
            # 经验筛选配置
            "experience": {
                "weight": 0.3,
                "is_hard_filter": False
            },
            
            # 毕业时间筛选配置
            "graduation": {
                "weight": 0.1,
                "is_hard_filter": True    # 启用硬性筛选
            },
            
            # 截止日期筛选配置
            "deadline": {
                "weight": 0.1,
                "is_hard_filter": True    # 启用硬性筛选
            }
        },
        
        "advanced": {
            "enabled": ["company_fame", "business_domain"],
            
            # 公司知名度筛选
            "company_fame": {
                "tier1_companies": [
                    "华为", "腾讯", "字节跳动", "阿里巴巴", "百度", "美团", 
                    "滴滴", "小米", "京东", "网易", "拼多多", "快手"
                ],
                "tier2_companies": [
                    "商汤", "旷视", "依图", "第四范式", "寒武纪", "地平线",
                    "云从", "澎思", "思必驰", "出门问问", "竹间智能"
                ],
                "tier3_companies": [
                    "搜狗", "360", "新浪", "搜狐", "爱奇艺", "优酷", 
                    "携程", "去哪儿", "同程", "马蜂窝"
                ],
                "weight": 0.4
            },
            
            # 业务领域筛选
            "business_domain": {
                "core_domains": [
                    "大模型", "LLM", "GPT", "ChatGPT", "Claude", "语言模型",
                    "AIGC", "生成式AI", "多模态", "预训练"
                ],
                "ai_domains": [
                    "机器学习", "深度学习", "人工智能", "AI", "神经网络",
                    "自然语言处理", "NLP", "计算机视觉", "CV"
                ],
                "related_domains": [
                    "算法", "数据科学", "推荐系统", "搜索引擎", "语音识别",
                    "知识图谱", "强化学习", "联邦学习"
                ],
                "weight": 0.6
            }
        },
        
        # 分数权重配置
        "score_weights": {
            "basic": 0.6,      # 基础筛选权重
            "advanced": 0.4    # 高级筛选权重
        }
    }

def load_unified_filter_config(config_path: str = None) -> Dict[str, Any]:
    """加载统一筛选配置"""
    default_config = get_unified_filter_config()
    
    if config_path and os.path.exists(config_path):
        try:
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                user_config = yaml.safe_load(f)
            
            # 深度合并配置
            def deep_merge(default, user):
                for key, value in user.items():
                    if key in default and isinstance(default[key], dict) and isinstance(value, dict):
                        deep_merge(default[key], value)
                    else:
                        default[key] = value
            
            if user_config:
                deep_merge(default_config, user_config)
            print(f"✅ 成功加载用户配置: {config_path}")
            
        except Exception as e:
            print(f"⚠️ 配置文件加载失败，使用默认配置: {e}")
    
    return default_config

# ========== Notion字段优化 ==========

def create_optimized_notion_properties(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """创建优化后的Notion属性 - 只保留14个核心字段"""
    properties = {}
    
    # 1. 核心信息 (6个字段)
    if job_data.get("岗位名称"):
        properties["岗位名称"] = {
            "title": [{"text": {"content": job_data["岗位名称"]}}]
        }
    
    for field in ["公司名称", "薪资", "工作地点"]:
        if job_data.get(field):
            properties[field] = {
                "rich_text": [{"text": {"content": str(job_data[field])}}]
            }
    
    if job_data.get("岗位描述"):
        content = job_data["岗位描述"]
        if len(content) > 2000:
            content = content[:1997] + "..."
        properties["岗位描述"] = {
            "rich_text": [{"text": {"content": content}}]
        }
    
    if job_data.get("岗位链接"):
        properties["岗位链接"] = {"url": job_data["岗位链接"]}
    
    # 2. 筛选评分 (3个字段)
    if job_data.get("综合评分") is not None:
        properties["综合评分"] = {"number": job_data["综合评分"]}
    
    if job_data.get("推荐等级"):
        properties["推荐等级"] = {
            "select": {"name": job_data["推荐等级"]}
        }
    
    if job_data.get("匹配建议"):
        content = job_data["匹配建议"]
        if len(content) > 2000:
            content = content[:1997] + "..."
        properties["匹配建议"] = {
            "rich_text": [{"text": {"content": content}}]
        }
    
    # 3. 关键匹配信息 (3个字段)
    if job_data.get("经验要求"):
        properties["经验要求"] = {
            "rich_text": [{"text": {"content": job_data["经验要求"]}}]
        }
    
    # 毕业时间要求（使用标准化版本）
    graduation_req = job_data.get("毕业时间要求_标准化") or job_data.get("毕业时间要求", "")
    if graduation_req:
        properties["毕业时间要求"] = {
            "rich_text": [{"text": {"content": graduation_req}}]
        }
    
    # 招聘截止日期（优先使用标准化版本）
    deadline = job_data.get("招聘截止日期_标准化") or job_data.get("招聘截止日期", "")
    if deadline:
        try:
            # 尝试作为日期字段
            datetime.strptime(deadline, "%Y-%m-%d")
            properties["招聘截止日期"] = {
                "date": {"start": deadline}
            }
        except ValueError:
            # 格式错误时作为文本
            properties["招聘截止日期"] = {
                "rich_text": [{"text": {"content": deadline}}]
            }
    
    # 4. 补充信息 (2个字段)
    if job_data.get("发布平台"):
        properties["发布平台"] = {
            "select": {"name": job_data["发布平台"]}
        }
    
    if job_data.get("招募方向"):
        properties["招募方向"] = {
            "rich_text": [{"text": {"content": job_data["招募方向"]}}]
        }
    
    return properties

def get_optimized_notion_fields() -> Dict[str, str]:
    """获取优化后的Notion字段定义（共14个字段）"""
    return {
        # 核心信息 (6个)
        "岗位名称": "title",
        "公司名称": "rich_text",
        "薪资": "rich_text",
        "工作地点": "rich_text",
        "岗位描述": "rich_text",
        "岗位链接": "url",
        
        # 筛选评分 (3个)
        "综合评分": "number",
        "推荐等级": "select",
        "经验匹配建议": "rich_text",
        
        # 关键匹配信息 (3个)
        "经验要求": "rich_text",
        "毕业时间要求": "rich_text",
        "招聘截止日期": "date",  # 可以是date或rich_text
        
        # 补充信息 (2个)
        "发布平台": "select",
        "招募方向": "rich_text"
    }

# ========== 测试函数 ==========

def test_unified_filter_system():
    """测试统一筛选系统"""
    print("🧪 测试统一筛选系统...")
    print("=" * 60)
    
    # 加载配置
    config = get_unified_filter_config()
    filter_manager = UnifiedJobFilterManager(config)
    
    # 测试数据
    test_jobs = [
        {
            "岗位名称": "机器学习算法工程师",
            "公司名称": "华为技术有限公司",
            "工作地点": "北京",
            "薪资": "25-35k",
            "经验要求": "1-3年工作经验",
            "毕业时间要求": "2024届毕业生",
            "招聘截止日期": "2024-12-31",
            "岗位描述": "负责大模型相关算法研发，要求熟悉深度学习框架PyTorch，参与LLM预训练和微调工作",
            "招募方向": "大模型算法方向"
        },
        {
            "岗位名称": "高级算法工程师", 
            "公司名称": "某创业公司",
            "工作地点": "成都",
            "薪资": "15-20k",  # 薪资过低
            "经验要求": "5年以上经验",  # 经验要求过高
            "毕业时间要求": "2025届",  # 毕业时间不匹配
            "岗位描述": "负责传统推荐算法优化"
        },
        {
            "岗位名称": "AI工程师",
            "公司名称": "腾讯科技",
            "工作地点": "深圳", 
            "薪资": "30-45k",
            "经验要求": "应届毕业生",
            "毕业时间要求": "2024届",
            "招聘截止日期": "2024-01-01",  # 已过期
            "岗位描述": "参与AI大模型训练和推理优化工作，负责ChatGPT类产品的算法研发"
        },
        {
            "岗位名称": "深度学习工程师",
            "公司名称": "字节跳动",
            "工作地点": "北京",
            "薪资": "28-40k",
            "经验要求": "1-2年经验",
            "毕业时间要求": "2024届",
            "招聘截止日期": "2024-06-30",
            "岗位描述": "负责抖音推荐算法优化，机器学习模型训练",
            "招募方向": "推荐算法方向"
        }
    ]
    
    print(f"📋 原始测试数据: {len(test_jobs)} 个岗位")
    
    # 第一步：基础筛选
    print(f"\n" + "="*20 + " 基础筛选 " + "="*20)
    basic_filtered = filter_manager.apply_basic_filters(test_jobs)
    
    if not basic_filtered:
        print("❌ 没有岗位通过基础筛选")
        return
    
    # 第二步：高级筛选
    print(f"\n" + "="*20 + " 高级筛选 " + "="*20)
    final_filtered = filter_manager.apply_advanced_filters(basic_filtered)
    
    # 显示最终结果
    print(f"\n" + "="*20 + " 最终结果 " + "="*20)
    print(f"📊 筛选结果: {len(final_filtered)}/{len(test_jobs)} 个岗位通过筛选")
    
    if final_filtered:
        print(f"\n🏆 推荐岗位排序:")
        for i, job in enumerate(final_filtered, 1):
            print(f"\n{i}. {job['岗位名称']} - {job['公司名称']}")
            print(f"   📍 地点: {job.get('工作地点', 'N/A')}")
            print(f"   💰 薪资: {job.get('薪资', 'N/A')}")
            print(f"   📊 综合评分: {job['综合评分']}分")
            print(f"   ⭐ 推荐等级: {job['推荐等级']}")
            print(f"   💡 匹配建议: {job.get('匹配建议', 'N/A')}")
    
    print(f"\n✅ 统一筛选系统测试完成!")

if __name__ == "__main__":
    test_unified_filter_system()