# optimized_notion_writer.py - 优化的Notion写入器
"""
支持16字段结构的Notion写入器：
1. 精简字段结构（从24个减少到16个）
2. 增强筛选结果展示
3. 优化用户体验
4. 兼容筛选系统输出
"""
import json
import os
import glob
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional

try:
    from notion_client import Client
    HAS_NOTION_CLIENT = True
except ImportError:
    HAS_NOTION_CLIENT = False
    print("⚠️  请安装notion-client: pip install notion-client")

from dotenv import load_dotenv

# 加载环境变量
for env_path in [".env", "../.env", "../../.env"]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

class OptimizedNotionJobWriter:
    """优化的Notion岗位写入器 - 16字段版本"""
    
    def __init__(self):
        """初始化优化版Notion写入器"""
        if not HAS_NOTION_CLIENT:
            raise ImportError("请先安装notion-client: pip install notion-client")
        
        self.notion_token = os.getenv("NOTION_TOKEN")
        self.database_id = os.getenv("NOTION_DATABASE_ID")
        
        if not self.notion_token:
            raise ValueError("请在.env文件中设置NOTION_TOKEN")
        if not self.database_id:
            raise ValueError("请在.env文件中设置NOTION_DATABASE_ID")
        
        self.client = Client(auth=self.notion_token)
        print(f"✅ 优化版Notion客户端初始化成功（16字段模式）")
    
    def _create_optimized_notion_properties(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建优化后的Notion属性 - 16个核心字段"""
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
        
        # 2. 筛选评分 (2个字段)
        if job_data.get("综合评分") is not None:
            # 确保是数字类型
            score = job_data["综合评分"]
            if isinstance(score, (int, float)):
                properties["综合评分"] = {"number": score}
            elif isinstance(score, str) and score.isdigit():
                properties["综合评分"] = {"number": int(score)}
        
        if job_data.get("推荐等级"):
            properties["推荐等级"] = {
                "select": {"name": job_data["推荐等级"]}
            }
        
        # 3. 匹配分析 (2个字段)
        if job_data.get("经验要求"):
            properties["经验要求"] = {
                "rich_text": [{"text": {"content": job_data["经验要求"]}}]
            }
        
        if job_data.get("经验匹配建议"):
            content = job_data["经验匹配建议"]
            if len(content) > 2000:
                content = content[:1997] + "..."
            properties["经验匹配建议"] = {
                "rich_text": [{"text": {"content": content}}]
            }
        
        # 4. 时间信息 (2个字段) - 使用标准化版本
        graduation_req = job_data.get("毕业时间要求_标准化", "")
        if graduation_req:
            properties["毕业时间要求_标准化"] = {
                "rich_text": [{"text": {"content": graduation_req}}]
            }
        
        # 招聘截止日期（优先使用标准化版本）
        deadline = job_data.get("招聘截止日期_标准化", "")
        if deadline:
            try:
                # 尝试作为日期字段
                if deadline and len(deadline) == 10 and deadline.count('-') == 2:
                    datetime.strptime(deadline, "%Y-%m-%d")
                    properties["招聘截止日期_标准化"] = {
                        "date": {"start": deadline}
                    }
                else:
                    # 格式错误时作为文本
                    properties["招聘截止日期_标准化"] = {
                        "rich_text": [{"text": {"content": deadline}}]
                    }
            except ValueError:
                # 格式错误时作为文本
                properties["招聘截止日期_标准化"] = {
                    "rich_text": [{"text": {"content": deadline}}]
                }
        
        # 5. 补充信息 (2个字段)
        if job_data.get("发布平台"):
            properties["发布平台"] = {
                "select": {"name": job_data["发布平台"]}
            }
        
        if job_data.get("招募方向"):
            properties["招募方向"] = {
                "rich_text": [{"text": {"content": job_data["招募方向"]}}]
            }
        
        # 6. HR和抓取信息 (2个字段)
        if job_data.get("HR活跃度"):
            properties["HR活跃度"] = {
                "rich_text": [{"text": {"content": job_data["HR活跃度"]}}]
            }
        
        if job_data.get("页面抓取时间"):
            crawl_time = job_data["页面抓取时间"]
            try:
                # 尝试作为日期字段
                if crawl_time and len(str(crawl_time)) >= 10:
                    # 提取日期部分 (YYYY-MM-DD)
                    date_part = str(crawl_time)[:10]
                    if date_part.count('-') == 2:
                        datetime.strptime(date_part, "%Y-%m-%d")
                        properties["页面抓取时间"] = {
                            "date": {"start": date_part}
                        }
                    else:
                        # 格式错误时作为文本
                        properties["页面抓取时间"] = {
                            "rich_text": [{"text": {"content": str(crawl_time)}}]
                        }
                else:
                    # 空值或格式错误时作为文本
                    properties["页面抓取时间"] = {
                        "rich_text": [{"text": {"content": str(crawl_time)}}]
                    }
            except (ValueError, TypeError):
                # 格式错误时作为文本
                properties["页面抓取时间"] = {
                    "rich_text": [{"text": {"content": str(crawl_time)}}]
                }
        
        return properties
    
    def get_optimized_notion_fields(self) -> Dict[str, str]:
        """获取优化后的Notion字段定义（共16个字段）"""
        return {
            # 核心信息 (6个)
            "岗位名称": "title",
            "公司名称": "rich_text",
            "薪资": "rich_text",
            "工作地点": "rich_text",
            "岗位描述": "rich_text",
            "岗位链接": "url",
            
            # 筛选评分 (2个)
            "综合评分": "number",
            "推荐等级": "select",
            
            # 匹配分析 (2个)
            "经验要求": "rich_text",
            "经验匹配建议": "rich_text",
            
            # 时间信息 (2个)
            "毕业时间要求_标准化": "rich_text",
            "招聘截止日期_标准化": "date",  # 可以是date或rich_text
            
            # 补充信息 (2个)
            "发布平台": "select",
            "招募方向": "rich_text",
            
            # HR和抓取信息 (2个)
            "HR活跃度": "rich_text",
            "页面抓取时间": "date"  # 可以是date或rich_text
        }
    
    async def create_page_optimized(self, job_data: Dict[str, Any]) -> Optional[str]:
        """在Notion数据库中创建新页面（优化版）"""
        try:
            properties = self._create_optimized_notion_properties(job_data)
            
            if not properties.get("岗位名称"):
                print("⚠️  岗位名称为空，跳过创建")
                return None
            
            response = self.client.pages.create(
                parent={"database_id": self.database_id},
                properties=properties
            )
            
            page_id = response["id"]
            job_title = job_data.get("岗位名称", "未知岗位")
            company = job_data.get("公司名称", "未知公司")
            score = job_data.get("综合评分", 0)
            level = job_data.get("推荐等级", "")
            
            # 根据推荐等级显示不同的成功信息
            if "强烈推荐" in level:
                print(f"🌟 创建成功【强推】: {job_title} - {company} ({score}分)")
            elif "推荐" in level:
                print(f"✨ 创建成功【推荐】: {job_title} - {company} ({score}分)")
            elif "可考虑" in level:
                print(f"⚠️ 创建成功【可考虑】: {job_title} - {company} ({score}分)")
            else:
                print(f"✅ 创建成功: {job_title} - {company} ({score}分)")
            
            return page_id
            
        except Exception as e:
            print(f"❌ 创建页面失败: {e}")
            print(f"   岗位: {job_data.get('岗位名称', 'N/A')}")
            return None
    
    async def batch_write_jobs_optimized(self, jobs_data: List[Dict[str, Any]], max_concurrent: int = 2) -> Dict[str, int]:
        """批量写入优化的岗位数据"""
        print(f"🚀 开始批量写入 {len(jobs_data)} 个岗位到Notion（16字段优化版）")
        
        stats = {
            "total": len(jobs_data),
            "success": 0,
            "failed": 0,
            "strongly_recommended": 0,  # 🌟 强烈推荐
            "recommended": 0,           # ✨ 推荐
            "considerable": 0,          # ⚠️ 可考虑
            "not_recommended": 0        # ❌ 不推荐
        }
        
        # 控制并发数量，避免API限流
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def write_single_job(job_data):
            async with semaphore:
                result = await self.create_page_optimized(job_data)
                if result:
                    stats["success"] += 1
                    
                    # 统计推荐等级
                    level = job_data.get("推荐等级", "")
                    if "强烈推荐" in level:
                        stats["strongly_recommended"] += 1
                    elif "推荐" in level:
                        stats["recommended"] += 1
                    elif "可考虑" in level:
                        stats["considerable"] += 1
                    else:
                        stats["not_recommended"] += 1
                else:
                    stats["failed"] += 1
                
                # 添加延迟避免API限流
                await asyncio.sleep(1)
                return result
        
        # 并发执行
        tasks = [write_single_job(job) for job in jobs_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 统计结果
        for result in results:
            if isinstance(result, Exception):
                stats["failed"] += 1
                print(f"⚠️  任务执行异常: {result}")
        
        return stats
    
    def check_database_schema(self) -> bool:
        """检查数据库结构是否匹配（优化版16字段）"""
        try:
            database = self.client.databases.retrieve(database_id=self.database_id)
            properties = database["properties"]
            
            print("📋 当前数据库字段:")
            for prop_name, prop_info in properties.items():
                prop_type = prop_info["type"]
                print(f"   {prop_name}: {prop_type}")
            
            # 检查必需的16个字段
            required_fields = self.get_optimized_notion_fields()
            
            missing_fields = []
            type_mismatches = []
            
            for field_name, expected_type in required_fields.items():
                if field_name not in properties:
                    missing_fields.append(f"{field_name} ({expected_type})")
                else:
                    actual_type = properties[field_name]["type"]
                    # 招聘截止日期可以是date或rich_text
                    if field_name == "招聘截止日期_标准化":
                        if actual_type not in ["date", "rich_text"]:
                            type_mismatches.append(f"{field_name}: 期望date/rich_text，实际{actual_type}")
                    elif actual_type != expected_type:
                        type_mismatches.append(f"{field_name}: 期望{expected_type}，实际{actual_type}")
            
            if missing_fields:
                print(f"❌ 缺少以下字段:")
                for field in missing_fields:
                    print(f"   - {field}")
                print(f"\n💡 请在Notion数据库中添加这些字段")
                self._print_schema_guide()
                return False
            
            if type_mismatches:
                print(f"⚠️  字段类型不匹配:")
                for mismatch in type_mismatches:
                    print(f"   - {mismatch}")
            
            print("✅ 优化数据库结构检查通过")
            print(f"📊 字段统计: 当前{len(properties)}个，需要{len(required_fields)}个，精简{len(properties)-len(required_fields)}个")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据库检查失败: {e}")
            return False
    
    def _print_schema_guide(self):
        """打印数据库结构指南"""
        print(f"\n📋 优化版Notion数据库结构指南（16个字段）:")
        
        fields_guide = {
            "核心信息 (6个)": {
                "岗位名称": "标题 (Title)",
                "公司名称": "文本 (Text)",
                "薪资": "文本 (Text)",
                "工作地点": "文本 (Text)",
                "岗位描述": "文本 (Text)",
                "岗位链接": "网址 (URL)"
            },
            "筛选评分 (2个)": {
                "综合评分": "数字 (Number)",
                "推荐等级": "选择 (Select) - 选项: 🌟 强烈推荐, ✨ 推荐, ⚠️ 可考虑, ❌ 不推荐"
            },
            "匹配分析 (2个)": {
                "经验要求": "文本 (Text)",
                "经验匹配建议": "文本 (Text)"
            },
            "时间信息 (2个)": {
                "毕业时间要求_标准化": "文本 (Text)",
                "招聘截止日期_标准化": "日期 (Date) 或 文本 (Text)"
            },
            "补充信息 (2个)": {
                "发布平台": "选择 (Select) - 选项: Boss直聘, 智联招聘, 猎聘, 拉勾等",
                "招募方向": "文本 (Text)"
            },
            "HR和抓取信息 (2个)": {
                "HR活跃度": "文本 (Text)",
                "页面抓取时间": "日期 (Date) 或 文本 (Text)"
            }
        }
        
        for category, fields in fields_guide.items():
            print(f"\n{category}:")
            for field_name, field_type in fields.items():
                print(f"   • {field_name}: {field_type}")
        
        print(f"\n💡 相比原版本，优化后减少了以下字段:")
        removed_fields = [
            "发布日期", "发布日期来源",
            "毕业时间要求", "毕业时间_匹配状态", "招聘截止日期", 
            "招聘截止日期_状态", "毕业时间要求_标准化", "提取时间"
        ]
        for field in removed_fields:
            print(f"   × {field}")


def find_latest_optimized_data():
    """查找最新的优化版岗位数据文件"""
    patterns = [
        "**/optimized_extraction_*.json",
        "**/filtered_jobs_*.json",
        "**/enhanced_extraction_*.json",
        "**/enhanced_notion_jobs_*.json"
    ]
    
    all_files = []
    for pattern in patterns:
        all_files.extend(glob.glob(pattern, recursive=True))
    
    if not all_files:
        print("❌ 没有找到优化版岗位数据文件")
        print("💡 请先运行带筛选的流水线: python integrated_pipeline_with_filters.py")
        return None
    
    latest_file = max(all_files, key=os.path.getmtime)
    print(f"📁 找到最新优化版数据文件: {latest_file}")
    return latest_file

def load_optimized_job_data(file_path: str) -> List[Dict[str, Any]]:
    """加载优化版岗位数据"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print("❌ 数据格式错误，期望列表格式")
            return []
        
        print(f"✅ 成功加载 {len(data)} 个优化版岗位")
        return data
        
    except Exception as e:
        print(f"❌ 加载数据失败: {e}")
        return []

def preview_optimized_jobs(jobs_data: List[Dict[str, Any]], limit: int = 5):
    """预览优化版岗位数据"""
    print(f"\n📋 优化版岗位预览 (前{min(limit, len(jobs_data))}个):")
    
    # 统计推荐等级分布
    level_stats = {}
    for job in jobs_data:
        level = job.get('推荐等级', '未知')
        level_stats[level] = level_stats.get(level, 0) + 1
    
    print(f"\n📊 推荐等级分布:")
    for level, count in level_stats.items():
        percentage = (count / len(jobs_data)) * 100 if jobs_data else 0
        print(f"   {level}: {count}个 ({percentage:.1f}%)")
    
    for i, job in enumerate(jobs_data[:limit], 1):
        job_name = job.get('岗位名称', 'N/A')
        company = job.get('公司名称', 'N/A')
        score = job.get('综合评分', 'N/A')
        level = job.get('推荐等级', 'N/A')
        experience_advice = job.get('经验匹配建议', 'N/A')
        
        print(f"\n  {i}. {job_name} - {company}")
        print(f"     📊 综合评分: {score}分 | ⭐ 推荐等级: {level}")
        print(f"     💡 经验建议: {experience_advice}")

async def test_optimized_notion_connection():
    """测试优化版Notion连接"""
    print("🧪 测试优化版Notion连接...")
    
    try:
        writer = OptimizedNotionJobWriter()
        
        # 检查数据库结构
        schema_ok = writer.check_database_schema()
        
        if not schema_ok:
            print("\n💡 请在Notion中创建数据库，包含以下16个字段:")
            writer._print_schema_guide()
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Notion连接失败: {e}")
        return False

async def main():
    """主函数"""
    print("🚀 优化版Notion岗位数据写入工具（16字段版本）")
    print("=" * 80)
    
    # 1. 检查环境变量
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("NOTION_DATABASE_ID")
    
    if not notion_token:
        print("❌ 请在.env文件中设置NOTION_TOKEN")
        print("💡 获取Token: https://www.notion.so/my-integrations")
        return
    
    if not database_id:
        print("❌ 请在.env文件中设置NOTION_DATABASE_ID")
        print("💡 数据库ID是URL中database/后面的部分")
        return
    
    print(f"✅ Notion配置检查通过")
    
    # 2. 测试连接
    connection_ok = await test_optimized_notion_connection()
    if not connection_ok:
        return
    
    # 3. 查找优化版数据文件
    data_file = find_latest_optimized_data()
    if not data_file:
        return
    
    # 4. 加载数据
    jobs_data = load_optimized_job_data(data_file)
    if not jobs_data:
        return
    
    # 5. 预览数据
    preview_optimized_jobs(jobs_data)
    
    # 6. 统计优化版信息
    strongly_recommended = sum(1 for job in jobs_data if "强烈推荐" in job.get("推荐等级", ""))
    recommended = sum(1 for job in jobs_data if "推荐" in job.get("推荐等级", "") and "强烈" not in job.get("推荐等级", ""))
    considerable = sum(1 for job in jobs_data if "可考虑" in job.get("推荐等级", ""))
    
    print(f"\n📊 岗位质量统计:")
    print(f"   🌟 强烈推荐: {strongly_recommended}个")
    print(f"   ✨ 推荐: {recommended}个") 
    print(f"   ⚠️ 可考虑: {considerable}个")
    print(f"   📋 字段优化: 24个→16个核心字段")
    
    # 7. 确认写入
    confirm = input(f"\n是否将 {len(jobs_data)} 个优化版岗位写入Notion? (y/N): ").strip().lower()
    if confirm != 'y':
        print("❌ 用户取消操作")
        return
    
    # 8. 执行写入
    try:
        writer = OptimizedNotionJobWriter()
        stats = await writer.batch_write_jobs_optimized(jobs_data, max_concurrent=2)
        
        print(f"\n" + "=" * 80)
        print(f"🎉 优化版批量写入完成!")
        print("=" * 80)
        print(f"📊 写入统计:")
        print(f"   总数: {stats['total']}")
        print(f"   成功: {stats['success']}")
        print(f"   失败: {stats['failed']}")
        print(f"   成功率: {stats['success']/stats['total']*100:.1f}%")
        
        print(f"\n🎯 岗位质量分布:")
        print(f"   🌟 强烈推荐: {stats['strongly_recommended']}个")
        print(f"   ✨ 推荐: {stats['recommended']}个")
        print(f"   ⚠️ 可考虑: {stats['considerable']}个")
        print(f"   ❌ 不推荐: {stats['not_recommended']}个")
        
        if stats['strongly_recommended'] > 0:
            print(f"\n💡 建议优先关注 {stats['strongly_recommended']} 个强烈推荐的岗位！")
        
        print(f"\n📱 Notion使用指南:")
        print(f"   1. 按\"综合评分\"列降序排列查看最优岗位")
        print(f"   2. 筛选\"推荐等级\" = \"🌟 强烈推荐\"查看顶级岗位")
        print(f"   3. 查看\"经验匹配建议\"制定申请策略")
        print(f"   4. 关注\"招聘截止日期_标准化\"合理安排时间")
        
    except Exception as e:
        print(f"❌ 批量写入失败: {e}")

if __name__ == "__main__":
    if not HAS_NOTION_CLIENT:
        print("请先安装notion-client:")
        print("pip install notion-client")
    else:
        asyncio.run(main())