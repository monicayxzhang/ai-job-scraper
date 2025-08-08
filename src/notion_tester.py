# test_notion.py - Notion写入专用测试器
"""
Notion写入功能的独立测试器，提供灵活的测试选项：
1. 连接测试 - 验证Token和数据库配置
2. 结构测试 - 检查数据库字段是否完整
3. 数据测试 - 使用模拟数据或文件数据测试写入
4. 诊断模式 - 逐步排查问题
5. 性能测试 - 测试批量写入性能
"""
import asyncio
import json
import os
import glob
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional

try:
    from optimized_notion_writer import OptimizedNotionJobWriter
    from dotenv import load_dotenv
    
    # 加载环境变量
    for env_path in [".env", "../.env", "../../.env"]:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break
    
    HAS_DEPENDENCIES = True
except ImportError as e:
    print(f"❌ 依赖导入失败: {e}")
    HAS_DEPENDENCIES = False

class NotionTester:
    """Notion写入功能测试器"""
    
    def __init__(self):
        self.writer = None
        self.test_results = []
        
    def create_mock_job_data(self, count: int = 5, scenario: str = "normal") -> List[Dict[str, Any]]:
        """创建模拟岗位数据"""
        
        if scenario == "empty":
            return []
        
        if scenario == "invalid":
            return [
                {"invalid_field": "invalid_data"},
                {"岗位名称": ""},  # 空标题
                {}  # 完全空的数据
            ]
        
        # 正常测试数据
        base_jobs = [
            {
                "岗位名称": "机器学习算法工程师",
                "公司名称": "华为技术有限公司",
                "薪资": "25-35k·13薪",
                "工作地点": "北京",
                "岗位描述": "负责大模型算法研发，参与LLM预训练和微调工作。要求熟悉PyTorch、TensorFlow等深度学习框架，有推荐系统相关经验优先。",
                "岗位链接": "https://www.zhipin.com/job_detail/test123.html",
                "综合评分": 85,
                "推荐等级": "🌟 强烈推荐",
                "经验要求": "1-3年工作经验",
                "经验匹配建议": "经验完全符合要求，强烈推荐申请",
                "毕业时间要求_标准化": "2024届",
                "招聘截止日期_标准化": "2024-12-31",
                "发布平台": "Boss直聘",
                "招募方向": "大模型算法方向"
            },
            {
                "岗位名称": "深度学习工程师",
                "公司名称": "腾讯科技",
                "薪资": "30-45k",
                "工作地点": "深圳",
                "岗位描述": "参与AI大模型训练和推理优化工作，负责ChatGPT类产品的算法研发。",
                "岗位链接": "https://www.zhipin.com/job_detail/test456.html",
                "综合评分": 78,
                "推荐等级": "✨ 推荐",
                "经验要求": "应届毕业生",
                "经验匹配建议": "面向应届生，适合申请",
                "毕业时间要求_标准化": "2024届",
                "招聘截止日期_标准化": "2024-06-30",
                "发布平台": "Boss直聘",
                "招募方向": "多模态大模型方向"
            },
            {
                "岗位名称": "算法工程师",
                "公司名称": "字节跳动",
                "薪资": "28-40k·13薪",
                "工作地点": "北京",
                "岗位描述": "负责抖音推荐算法优化，机器学习模型训练和部署。",
                "岗位链接": "https://www.zhipin.com/job_detail/test789.html",
                "综合评分": 72,
                "推荐等级": "✨ 推荐",
                "经验要求": "1-2年经验",
                "经验匹配建议": "经验基本符合，可以申请",
                "毕业时间要求_标准化": "2024届",
                "招聘截止日期_标准化": "2024-08-15",
                "发布平台": "Boss直聘",
                "招募方向": "推荐算法方向"
            },
            {
                "岗位名称": "AI工程师",
                "公司名称": "某创业公司",
                "薪资": "20-25k",
                "工作地点": "杭州",
                "岗位描述": "负责人工智能产品的算法开发和优化工作。",
                "岗位链接": "https://www.zhipin.com/job_detail/test999.html",
                "综合评分": 58,
                "推荐等级": "⚠️ 可考虑",
                "经验要求": "2-3年经验",
                "经验匹配建议": "经验要求略高，可通过项目经验补充",
                "毕业时间要求_标准化": "2024届",
                "招聘截止日期_标准化": "2024-07-20",
                "发布平台": "Boss直聘",
                "招募方向": "AI产品化方向"
            },
            {
                "岗位名称": "研发工程师",
                "公司名称": "小公司",
                "薪资": "12-18k",
                "工作地点": "成都",
                "岗位描述": "参与软件产品的开发和维护工作。",
                "岗位链接": "https://www.zhipin.com/job_detail/test000.html",
                "综合评分": 42,
                "推荐等级": "❌ 不推荐",
                "经验要求": "5年以上经验",
                "经验匹配建议": "经验要求过高，薪资偏低，不建议申请",
                "毕业时间要求_标准化": "经验不限",
                "招聘截止日期_标准化": "2024-09-30",
                "发布平台": "Boss直聘",
                "招募方向": "软件开发方向"
            }
        ]
        
        if scenario == "large":
            # 生成大量数据用于性能测试
            jobs = []
            for i in range(count):
                job = base_jobs[i % len(base_jobs)].copy()
                job["岗位名称"] = f"{job['岗位名称']}_{i+1}"
                job["岗位链接"] = f"https://www.zhipin.com/job_detail/test{i+1000}.html"
                jobs.append(job)
            return jobs
        
        # 返回指定数量的正常数据
        return base_jobs[:min(count, len(base_jobs))]
    
    async def test_environment_setup(self) -> bool:
        """测试环境配置"""
        print("🔧 检查环境配置...")
        
        # 检查环境变量
        notion_token = os.getenv("NOTION_TOKEN")
        database_id = os.getenv("NOTION_DATABASE_ID")
        
        if not notion_token:
            print("❌ NOTION_TOKEN 未设置")
            print("💡 请在 .env 文件中设置: NOTION_TOKEN=your_token_here")
            return False
        
        if not database_id:
            print("❌ NOTION_DATABASE_ID 未设置")
            print("💡 请在 .env 文件中设置: NOTION_DATABASE_ID=your_database_id")
            return False
        
        print(f"✅ NOTION_TOKEN: {notion_token[:10]}...{notion_token[-4:]}")
        print(f"✅ NOTION_DATABASE_ID: {database_id[:8]}...{database_id[-4:]}")
        return True
    
    async def test_connection_only(self) -> bool:
        """仅测试Notion连接"""
        print("🔗 测试Notion API连接...")
        
        try:
            self.writer = OptimizedNotionJobWriter()
            print("✅ Notion客户端初始化成功")
            return True
        except Exception as e:
            print(f"❌ Notion连接失败: {e}")
            return False
    
    async def test_database_schema(self) -> bool:
        """测试数据库结构"""
        print("📋 检查数据库结构...")
        
        if not self.writer:
            if not await self.test_connection_only():
                return False
        
        try:
            schema_ok = self.writer.check_database_schema()
            if schema_ok:
                print("✅ 数据库结构检查通过")
            else:
                print("❌ 数据库结构不完整")
                print("💡 请按照提示在Notion中添加缺少的字段")
            return schema_ok
        except Exception as e:
            print(f"❌ 数据库结构检查失败: {e}")
            return False
    
    async def test_write_with_mock_data(self, count: int = 3, scenario: str = "normal") -> bool:
        """使用模拟数据测试写入"""
        print(f"🧪 使用模拟数据测试写入 (场景: {scenario}, 数量: {count})...")
        
        if not self.writer:
            if not await self.test_connection_only():
                return False
        
        # 创建测试数据
        test_jobs = self.create_mock_job_data(count, scenario)
        
        if not test_jobs:
            print("⚠️  测试数据为空")
            return True
        
        # 预览测试数据
        print(f"\n📋 测试数据预览:")
        for i, job in enumerate(test_jobs[:3], 1):
            title = job.get('岗位名称', 'N/A')
            company = job.get('公司名称', 'N/A')
            score = job.get('综合评分', 'N/A')
            level = job.get('推荐等级', 'N/A')
            print(f"  {i}. {title} - {company}")
            print(f"     评分: {score} | 等级: {level}")
        
        # 询问确认
        if scenario != "dry_run":
            confirm = input(f"\n是否写入 {len(test_jobs)} 个测试岗位到Notion? (y/N): ").strip().lower()
            if confirm != 'y':
                print("❌ 用户取消测试")
                return False
        else:
            print("🔍 干运行模式，不实际写入")
            return True
        
        # 执行写入测试
        try:
            stats = await self.writer.batch_write_jobs_optimized(test_jobs, max_concurrent=1)
            
            print(f"\n📊 写入测试结果:")
            print(f"   总数: {stats['total']}")
            print(f"   成功: {stats['success']}")
            print(f"   失败: {stats['failed']}")
            
            if stats['total'] > 0:
                success_rate = stats['success'] / stats['total'] * 100
                print(f"   成功率: {success_rate:.1f}%")
            
            # 记录测试结果
            self.test_results.append({
                "test_type": "mock_data_write",
                "scenario": scenario,
                "stats": stats,
                "timestamp": datetime.now().isoformat()
            })
            
            return stats['success'] > 0
            
        except Exception as e:
            print(f"❌ 写入测试失败: {e}")
            return False
    
    async def test_write_with_file_data(self, file_path: str, sample_size: Optional[int] = None) -> bool:
        """使用文件数据测试写入"""
        print(f"📁 使用文件数据测试写入: {file_path}")
        
        if not os.path.exists(file_path):
            print(f"❌ 文件不存在: {file_path}")
            return False
        
        try:
            # 加载文件数据
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.endswith('.jsonl'):
                    jobs_data = [json.loads(line) for line in f if line.strip()]
                else:
                    jobs_data = json.load(f)
            
            if not isinstance(jobs_data, list):
                print("❌ 文件格式错误，期望列表格式")
                return False
            
            if not jobs_data:
                print("⚠️  文件数据为空")
                return True
            
            # 限制测试数量
            if sample_size and len(jobs_data) > sample_size:
                jobs_data = jobs_data[:sample_size]
                print(f"📏 限制测试数量为: {sample_size}")
            
            print(f"✅ 成功加载 {len(jobs_data)} 个岗位")
            
            # 数据预览
            print(f"\n📋 文件数据预览:")
            for i, job in enumerate(jobs_data[:3], 1):
                title = job.get('岗位名称', 'N/A')
                company = job.get('公司名称', 'N/A')
                score = job.get('综合评分', 'N/A')
                level = job.get('推荐等级', 'N/A')
                print(f"  {i}. {title} - {company}")
                if score != 'N/A':
                    print(f"     评分: {score} | 等级: {level}")
            
            # 执行写入测试
            return await self.test_write_with_mock_data(count=len(jobs_data), scenario="file_data")
            
        except Exception as e:
            print(f"❌ 加载文件数据失败: {e}")
            return False
    
    async def find_and_test_latest_data(self) -> bool:
        """查找并测试最新的数据文件"""
        print("🔍 查找最新的岗位数据文件...")
        
        # 查找数据文件 - 包括快照目录
        patterns = [
            # 流水线生成的数据文件
            "data/enhanced_pipeline_extracted_*.json",
            "data/filtered_jobs_*.json",
            "enhanced_extraction_*.json",
            "extracted_jobs_*.json",
            "optimized_extraction_*.json",
            
            # 快照系统中的数据
            "debug/snapshots/*_after_advanced_filter.json",
            "debug/snapshots/*_extraction_output.json", 
            "debug/snapshots/*_final_output.json",
            "debug/snapshots/*_after_extraction.json"
        ]
        
        all_files = []
        for pattern in patterns:
            found_files = glob.glob(pattern, recursive=True)
            all_files.extend(found_files)
        
        if not all_files:
            print("❌ 没有找到岗位数据文件")
            print("💡 尝试以下选项:")
            print("   1. 使用 --mock-data 选项测试模拟数据")
            print("   2. 先运行流水线生成数据")
            print("   3. 检查 debug/snapshots/ 目录")
            return False
        
        # 按修改时间排序，找最新的
        latest_file = max(all_files, key=os.path.getmtime)
        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(latest_file))
        
        print(f"📁 找到最新数据文件: {latest_file}")
        print(f"⏰ 文件时间: {file_age.total_seconds()/60:.1f} 分钟前")
        
        # 检查文件类型
        if "snapshots" in latest_file:
            print("📸 数据来源: 流水线快照")
        else:
            print("💾 数据来源: 直接输出文件")
        
        return await self.test_write_with_file_data(latest_file)
    
    async def find_and_test_snapshot_data(self, snapshot_stage: str = None) -> bool:
        """从快照数据中查找并测试"""
        print("📸 查找快照数据...")
        
        snapshot_dir = "debug/snapshots"
        if not os.path.exists(snapshot_dir):
            print(f"❌ 快照目录不存在: {snapshot_dir}")
            print("💡 请先运行流水线生成快照数据")
            return False
        
        # 如果指定了阶段，查找特定阶段的快照
        if snapshot_stage:
            pattern = f"{snapshot_dir}/*_{snapshot_stage}.json"
            stage_files = glob.glob(pattern)
            
            if not stage_files:
                print(f"❌ 没有找到阶段 '{snapshot_stage}' 的快照")
                print("💡 可用的快照阶段:")
                self._list_available_snapshots()
                return False
            
            latest_file = max(stage_files, key=os.path.getmtime)
        else:
            # 查找所有可能包含岗位数据的快照
            candidate_patterns = [
                f"{snapshot_dir}/*_after_advanced_filter.json",
                f"{snapshot_dir}/*_extraction_output.json",
                f"{snapshot_dir}/*_final_output.json",
                f"{snapshot_dir}/*_after_extraction.json"
            ]
            
            all_candidates = []
            for pattern in candidate_patterns:
                all_candidates.extend(glob.glob(pattern))
            
            if not all_candidates:
                print("❌ 没有找到包含岗位数据的快照")
                print("💡 可用的快照文件:")
                self._list_available_snapshots()
                return False
            
            latest_file = max(all_candidates, key=os.path.getmtime)
        
        print(f"📁 使用快照文件: {os.path.basename(latest_file)}")
        
        # 尝试从快照中提取数据
        return await self._test_snapshot_data(latest_file)
    
    async def _test_snapshot_data(self, snapshot_file: str) -> bool:
        """测试快照数据"""
        try:
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                snapshot_data = json.load(f)
            
            # 快照文件可能包含元数据，需要提取实际的岗位数据
            jobs_data = None
            
            if isinstance(snapshot_data, list):
                # 直接是岗位列表
                jobs_data = snapshot_data
            elif isinstance(snapshot_data, dict):
                # 可能是包含元数据的快照
                if 'successful_jobs' in snapshot_data:
                    jobs_data = snapshot_data['successful_jobs']
                elif 'data' in snapshot_data:
                    jobs_data = snapshot_data['data']
                elif 'jobs' in snapshot_data:
                    jobs_data = snapshot_data['jobs']
                else:
                    # 尝试找到最大的列表字段
                    for key, value in snapshot_data.items():
                        if isinstance(value, list) and len(value) > 0:
                            # 检查是否像岗位数据
                            if isinstance(value[0], dict) and '岗位名称' in value[0]:
                                jobs_data = value
                                break
            
            if not jobs_data:
                print("❌ 无法从快照中提取岗位数据")
                print("🔍 快照内容结构:")
                if isinstance(snapshot_data, dict):
                    for key in snapshot_data.keys():
                        print(f"   - {key}: {type(snapshot_data[key])}")
                return False
            
            if not isinstance(jobs_data, list):
                print("❌ 快照数据格式错误，期望列表格式")
                return False
            
            print(f"✅ 从快照中提取到 {len(jobs_data)} 个岗位")
            
            # 数据预览
            print(f"\n📋 快照数据预览:")
            for i, job in enumerate(jobs_data[:3], 1):
                title = job.get('岗位名称', 'N/A')
                company = job.get('公司名称', 'N/A')
                score = job.get('综合评分', 'N/A')
                level = job.get('推荐等级', 'N/A')
                print(f"  {i}. {title} - {company}")
                if score != 'N/A':
                    print(f"     评分: {score} | 等级: {level}")
            
            # 询问确认
            confirm = input(f"\n是否写入 {len(jobs_data)} 个快照岗位到Notion? (y/N): ").strip().lower()
            if confirm != 'y':
                print("❌ 用户取消测试")
                return False
            
            # 执行写入测试
            try:
                if not self.writer:
                    if not await self.test_connection_only():
                        return False
                
                stats = await self.writer.batch_write_jobs_optimized(jobs_data, max_concurrent=2)
                
                print(f"\n📊 快照数据写入结果:")
                print(f"   总数: {stats['total']}")
                print(f"   成功: {stats['success']}")
                print(f"   失败: {stats['failed']}")
                
                if stats['total'] > 0:
                    success_rate = stats['success'] / stats['total'] * 100
                    print(f"   成功率: {success_rate:.1f}%")
                
                return stats['success'] > 0
                
            except Exception as e:
                print(f"❌ 快照数据写入失败: {e}")
                return False
            
        except Exception as e:
            print(f"❌ 读取快照文件失败: {e}")
            return False
    
    def _list_available_snapshots(self) -> None:
        """列出可用的快照文件"""
        snapshot_dir = "debug/snapshots"
        if not os.path.exists(snapshot_dir):
            return
        
        snapshot_files = glob.glob(f"{snapshot_dir}/*.json")
        if not snapshot_files:
            print("   (没有找到快照文件)")
            return
        
        # 按时间排序
        snapshot_files.sort(key=os.path.getmtime, reverse=True)
        
        print("   最近的快照文件:")
        for file in snapshot_files[:10]:  # 只显示最近10个
            basename = os.path.basename(file)
            mod_time = datetime.fromtimestamp(os.path.getmtime(file))
            age = datetime.now() - mod_time
            print(f"     • {basename} ({age.total_seconds()/60:.1f}分钟前)")
    
    async def list_all_data_sources(self) -> None:
        """列出所有可用的数据源"""
        print("📋 扫描所有可用的数据源...")
        print("=" * 60)
        
        # 1. 快照数据
        print("1️⃣ 快照数据 (debug/snapshots/):")
        snapshot_dir = "debug/snapshots"
        if os.path.exists(snapshot_dir):
            self._list_available_snapshots()
        else:
            print("   ❌ 快照目录不存在")
        
        # 2. 直接输出文件
        print("\n2️⃣ 直接输出文件:")
        patterns = [
            "data/enhanced_pipeline_extracted_*.json",
            "data/filtered_jobs_*.json",
            "enhanced_extraction_*.json", 
            "extracted_jobs_*.json"
        ]
        
        output_files = []
        for pattern in patterns:
            output_files.extend(glob.glob(pattern, recursive=True))
        
        if output_files:
            output_files.sort(key=os.path.getmtime, reverse=True)
            for file in output_files[:5]:
                basename = os.path.basename(file)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file))
                age = datetime.now() - mod_time
                print(f"   • {basename} ({age.total_seconds()/60:.1f}分钟前)")
        else:
            print("   ❌ 没有找到输出文件")
        
        print(f"\n💡 使用建议:")
        print(f"   --latest-data     : 自动选择最新数据")
        print(f"   --snapshot-data   : 从快照中选择数据")
        print(f"   --file <path>     : 使用指定文件")
        print(f"   --mock-data       : 使用模拟数据（最安全）")
    
    async def run_diagnostic(self) -> None:
        """运行完整诊断"""
        print("🔍 运行Notion集成诊断...")
        print("=" * 60)
        
        # 步骤1: 环境检查
        print("\n1️⃣ 环境配置检查")
        env_ok = await self.test_environment_setup()
        if not env_ok:
            print("🛑 环境配置有问题，请先解决后重试")
            return
        
        # 步骤2: 连接测试
        print("\n2️⃣ API连接测试")
        conn_ok = await self.test_connection_only()
        if not conn_ok:
            print("🛑 API连接失败，请检查Token是否正确")
            return
        
        # 步骤3: 数据库结构检查
        print("\n3️⃣ 数据库结构检查")
        schema_ok = await self.test_database_schema()
        if not schema_ok:
            print("🛑 数据库结构不完整，请添加缺少的字段")
            return
        
        # 步骤4: 小规模写入测试
        print("\n4️⃣ 小规模写入测试")
        write_ok = await self.test_write_with_mock_data(count=1, scenario="normal")
        if not write_ok:
            print("🛑 写入测试失败，请检查权限和数据格式")
            return
        
        print("\n" + "=" * 60)
        print("🎉 所有诊断检查通过！Notion集成工作正常")
        print("💡 现在可以安全地运行完整的流水线")
    
    async def run_performance_test(self, job_count: int = 50) -> None:
        """运行性能测试"""
        print(f"⚡ 运行性能测试 (写入 {job_count} 个岗位)...")
        
        if not self.writer:
            if not await self.test_connection_only():
                return
        
        # 创建大量测试数据
        test_jobs = self.create_mock_job_data(job_count, "large")
        
        print(f"📊 准备写入 {len(test_jobs)} 个岗位...")
        
        confirm = input(f"⚠️  这将在Notion中创建 {job_count} 个测试记录，确认继续? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ 用户取消性能测试")
            return
        
        # 记录开始时间
        start_time = datetime.now()
        
        try:
            # 执行批量写入
            stats = await self.writer.batch_write_jobs_optimized(test_jobs, max_concurrent=3)
            
            # 计算性能指标
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"\n📊 性能测试结果:")
            print(f"   写入岗位: {stats['success']}/{stats['total']}")
            print(f"   耗时: {duration:.2f}秒")
            print(f"   平均速度: {stats['success']/duration:.2f} 岗位/秒")
            print(f"   成功率: {stats['success']/stats['total']*100:.1f}%")
            
            if stats['failed'] > 0:
                print(f"   失败数: {stats['failed']}")
                print("💡 如果失败率较高，建议降低并发数量")
            
        except Exception as e:
            print(f"❌ 性能测试失败: {e}")
    
    def print_test_summary(self) -> None:
        """打印测试摘要"""
        if not self.test_results:
            print("📋 没有测试记录")
            return
        
        print("\n📋 测试摘要:")
        print("-" * 40)
        
        for i, result in enumerate(self.test_results, 1):
            test_type = result['test_type']
            scenario = result.get('scenario', 'N/A')
            stats = result.get('stats', {})
            timestamp = result['timestamp']
            
            print(f"{i}. {test_type} ({scenario})")
            print(f"   时间: {timestamp}")
            if stats:
                print(f"   结果: {stats.get('success', 0)}/{stats.get('total', 0)} 成功")
            print()

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='Notion写入功能测试器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 完整诊断
  python test_notion.py --diagnose
  
  # 只测试连接
  python test_notion.py --connection-only
  
  # 只测试数据库结构  
  python test_notion.py --schema-only
  
  # 使用模拟数据测试写入
  python test_notion.py --mock-data --count 3
  
  # 使用指定文件测试
  python test_notion.py --file data/my_jobs.json --sample-size 5
  
  # 查找最新数据并测试
  python test_notion.py --latest-data
  
  # 使用快照数据测试
  python test_notion.py --snapshot-data
  
  # 使用特定阶段的快照
  python test_notion.py --snapshot-data after_advanced_filter
  
  # 列出所有可用数据源
  python test_notion.py --list-sources
  
  # 性能测试
  python test_notion.py --performance --count 50
  
  # 干运行（不实际写入）
  python test_notion.py --mock-data --dry-run
        """
    )
    
    # 测试模式选择
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--diagnose', action='store_true', help='运行完整诊断')
    group.add_argument('--connection-only', action='store_true', help='仅测试连接')
    group.add_argument('--schema-only', action='store_true', help='仅测试数据库结构')
    group.add_argument('--mock-data', action='store_true', help='使用模拟数据测试')
    group.add_argument('--file', type=str, help='使用指定文件测试')
    group.add_argument('--latest-data', action='store_true', help='使用最新数据文件测试')
    group.add_argument('--snapshot-data', type=str, nargs='?', const='auto', 
                       help='使用快照数据测试（可指定阶段，如 after_advanced_filter）')
    group.add_argument('--list-sources', action='store_true', help='列出所有可用数据源')
    group.add_argument('--performance', action='store_true', help='运行性能测试')
    
    # 参数选项
    parser.add_argument('--count', type=int, default=5, help='测试岗位数量')
    parser.add_argument('--sample-size', type=int, help='文件数据采样数量')
    parser.add_argument('--scenario', choices=['normal', 'invalid', 'empty'], 
                       default='normal', help='测试场景')
    parser.add_argument('--dry-run', action='store_true', help='干运行，不实际写入')
    
    return parser.parse_args()

async def main():
    """主函数"""
    if not HAS_DEPENDENCIES:
        print("❌ 请确保已安装必要的依赖")
        return
    
    args = parse_args()
    tester = NotionTester()
    
    print("🧪 Notion写入功能测试器")
    print("=" * 60)
    
    try:
        if args.diagnose:
            await tester.run_diagnostic()
        
        elif args.connection_only:
            success = await tester.test_connection_only()
            print(f"🔗 连接测试: {'✅ 成功' if success else '❌ 失败'}")
        
        elif args.schema_only:
            success = await tester.test_database_schema()
            print(f"📋 结构测试: {'✅ 通过' if success else '❌ 失败'}")
        
        elif args.mock_data:
            scenario = "dry_run" if args.dry_run else args.scenario
            success = await tester.test_write_with_mock_data(args.count, scenario)
            print(f"🧪 模拟数据测试: {'✅ 成功' if success else '❌ 失败'}")
        
        elif args.file:
            success = await tester.test_write_with_file_data(args.file, args.sample_size)
            print(f"📁 文件数据测试: {'✅ 成功' if success else '❌ 失败'}")
        
        elif args.latest_data:
            success = await tester.find_and_test_latest_data()
            print(f"🔍 最新数据测试: {'✅ 成功' if success else '❌ 失败'}")
        
        elif args.snapshot_data:
            stage = None if args.snapshot_data == 'auto' else args.snapshot_data
            success = await tester.find_and_test_snapshot_data(stage)
            print(f"📸 快照数据测试: {'✅ 成功' if success else '❌ 失败'}")
        
        elif args.list_sources:
            await tester.list_all_data_sources()
        
        elif args.performance:
            await tester.run_performance_test(args.count)
        
        else:
            # 默认：运行基础检查
            print("运行基础检查...")
            env_ok = await tester.test_environment_setup()
            if env_ok:
                conn_ok = await tester.test_connection_only()
                if conn_ok:
                    await tester.test_database_schema()
            
            print("\n💡 使用 --help 查看更多测试选项")
        
        # 显示测试摘要
        tester.print_test_summary()
        
    except KeyboardInterrupt:
        print("\n⚠️  测试被用户中断")
    except Exception as e:
        print(f"\n❌ 测试执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())