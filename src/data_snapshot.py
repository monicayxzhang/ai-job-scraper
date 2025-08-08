# data_snapshot.py - 数据快照系统
import json
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from src.logger_config import get_logger

class DataSnapshot:
    """数据快照管理器"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.snapshots: Dict[str, Dict[str, Any]] = {}
        self.snapshot_dir = "debug/snapshots"
        
        # 确保快照目录存在
        os.makedirs(self.snapshot_dir, exist_ok=True)
        
        self.logger = get_logger()
        self.logger.debug("数据快照系统初始化", {"session_id": session_id})
    
    def capture(self, stage: str, data: Any, metadata: Optional[Dict] = None) -> str:
        """
        捕获关键阶段的数据快照
        
        Args:
            stage: 阶段名称 (如 "raw_crawl", "dedup_input", "dedup_output")
            data: 要快照的数据
            metadata: 额外的元数据信息
            
        Returns:
            快照文件路径
        """
        timestamp = datetime.now().isoformat()
        metadata = metadata or {}
        
        # 生成快照摘要
        snapshot_summary = {
            "stage": stage,
            "timestamp": timestamp,
            "metadata": metadata,
            "data_summary": self._summarize_data(data),
            "sample_data": self._get_sample_data(data)
        }
        
        # 保存到内存索引
        self.snapshots[stage] = snapshot_summary
        
        # 保存详细数据到文件
        detail_file = os.path.join(self.snapshot_dir, f"{self.session_id}_{stage}.json")
        
        try:
            with open(detail_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=self._json_serializer)
            
            # 记录快照信息
            data_count = len(data) if isinstance(data, (list, dict)) else 1
            self.logger.debug(f"数据快照已保存: {stage}", {
                "file_path": detail_file,
                "data_count": data_count,
                "file_size_kb": round(os.path.getsize(detail_file) / 1024, 2)
            })
            
            # 控制台简要信息
            if hasattr(self.logger, 'level') and self.logger.level.value in ['debug', 'trace']:
                print(f"📸 快照保存: {stage} ({data_count} 项) -> {os.path.basename(detail_file)}")
            
            return detail_file
            
        except Exception as e:
            self.logger.error(f"保存快照失败: {stage}", {
                "error": str(e),
                "stage": stage,
                "data_type": type(data).__name__
            }, e)
            return ""
    
    def _summarize_data(self, data: Any) -> Dict[str, Any]:
        """生成数据摘要"""
        if isinstance(data, list):
            summary = {
                "type": "list",
                "count": len(data)
            }
            
            if data and isinstance(data[0], dict):
                summary["sample_keys"] = list(data[0].keys())
            elif data:
                summary["sample_type"] = type(data[0]).__name__
            
            return summary
            
        elif isinstance(data, dict):
            return {
                "type": "dict",
                "keys": list(data.keys()),
                "key_count": len(data)
            }
        else:
            return {
                "type": type(data).__name__,
                "value_preview": str(data)[:100] + "..." if len(str(data)) > 100 else str(data)
            }
    
    def _get_sample_data(self, data: Any, sample_size: int = 2) -> Any:
        """获取样本数据"""
        if isinstance(data, list):
            return data[:sample_size]
        elif isinstance(data, dict):
            return {k: v for i, (k, v) in enumerate(data.items()) if i < sample_size}
        else:
            return data
    
    def _json_serializer(self, obj):
        """JSON序列化辅助函数"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        # 其他不可序列化的对象转为字符串
        return str(obj)
    
    def save_summary(self) -> str:
        """保存快照摘要索引"""
        summary_file = os.path.join(self.snapshot_dir, f"{self.session_id}_summary.json")
        
        try:
            # 添加会话级别的元数据
            summary_data = {
                "session_info": {
                    "session_id": self.session_id,
                    "created_at": datetime.now().isoformat(),
                    "total_snapshots": len(self.snapshots)
                },
                "snapshots": self.snapshots
            }
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)
            
            self.logger.success(f"快照摘要已保存: {summary_file}", {
                "snapshot_count": len(self.snapshots),
                "stages": list(self.snapshots.keys())
            })
            
            # 创建最新摘要的软链接
            latest_summary = os.path.join(self.snapshot_dir, "latest_summary.json")
            self._create_symlink(summary_file, latest_summary)
            
            return summary_file
            
        except Exception as e:
            self.logger.error("保存快照摘要失败", {"error": str(e)}, e)
            return ""
    
    def _create_symlink(self, target: str, link_name: str):
        """创建软链接（跨平台兼容）"""
        try:
            if os.path.exists(link_name):
                os.remove(link_name)
            
            # 尝试创建软链接
            try:
                os.symlink(os.path.basename(target), link_name)
            except (OSError, NotImplementedError):
                # Windows可能不支持symlink，直接复制
                import shutil
                shutil.copy2(target, link_name)
                
        except Exception as e:
            self.logger.warning(f"创建软链接失败: {link_name}", {"error": str(e)})
    
    def load_snapshot(self, stage: str) -> Optional[Any]:
        """加载指定阶段的快照数据"""
        snapshot_file = os.path.join(self.snapshot_dir, f"{self.session_id}_{stage}.json")
        
        if not os.path.exists(snapshot_file):
            self.logger.warning(f"快照文件不存在: {stage}", {"file_path": snapshot_file})
            return None
        
        try:
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.logger.debug(f"快照加载成功: {stage}", {
                "file_path": snapshot_file,
                "data_type": type(data).__name__
            })
            
            return data
            
        except Exception as e:
            self.logger.error(f"加载快照失败: {stage}", {
                "file_path": snapshot_file,
                "error": str(e)
            }, e)
            return None
    
    def list_snapshots(self) -> List[str]:
        """列出当前会话的所有快照"""
        pattern = f"{self.session_id}_*.json"
        snapshot_files = []
        
        for filename in os.listdir(self.snapshot_dir):
            if filename.startswith(f"{self.session_id}_") and filename.endswith('.json'):
                # 提取阶段名称
                stage = filename[len(f"{self.session_id}_"):-5]  # 移除前缀和.json后缀
                if stage != "summary":  # 排除摘要文件
                    snapshot_files.append(stage)
        
        return sorted(snapshot_files)
    
    def compare_snapshots(self, stage1: str, stage2: str) -> Dict[str, Any]:
        """对比两个快照的数据差异"""
        data1 = self.load_snapshot(stage1)
        data2 = self.load_snapshot(stage2)
        
        if data1 is None or data2 is None:
            return {"error": "无法加载快照数据"}
        
        comparison = {
            "stage1": stage1,
            "stage2": stage2,
            "comparison_time": datetime.now().isoformat()
        }
        
        # 如果都是列表，对比数量和内容
        if isinstance(data1, list) and isinstance(data2, list):
            comparison.update({
                "type": "list_comparison",
                "stage1_count": len(data1),
                "stage2_count": len(data2),
                "count_diff": len(data2) - len(data1)
            })
            
            # 如果是字典列表，对比键值
            if data1 and isinstance(data1[0], dict) and data2 and isinstance(data2[0], dict):
                keys1 = set(data1[0].keys()) if data1 else set()
                keys2 = set(data2[0].keys()) if data2 else set()
                
                comparison.update({
                    "keys_added": list(keys2 - keys1),
                    "keys_removed": list(keys1 - keys2),
                    "common_keys": list(keys1 & keys2)
                })
        
        elif isinstance(data1, dict) and isinstance(data2, dict):
            keys1 = set(data1.keys())
            keys2 = set(data2.keys())
            
            comparison.update({
                "type": "dict_comparison",
                "stage1_keys": len(keys1),
                "stage2_keys": len(keys2),
                "keys_added": list(keys2 - keys1),
                "keys_removed": list(keys1 - keys2),
                "common_keys": list(keys1 & keys2)
            })
        
        self.logger.debug(f"快照对比完成: {stage1} vs {stage2}", comparison)
        return comparison

# 便捷函数
def create_snapshot_manager(session_id: Optional[str] = None) -> DataSnapshot:
    """创建数据快照管理器"""
    if session_id is None:
        # 从全局logger获取session_id
        logger = get_logger()
        session_id = getattr(logger, 'session_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
    
    return DataSnapshot(session_id)

# 使用示例和测试
if __name__ == "__main__":
    from src.logger_config import init_logger, LogLevel
    
    print("🧪 测试数据快照系统...")
    
    # 初始化日志系统
    logger = init_logger(LogLevel.DEBUG)
    
    # 创建快照管理器
    snapshot = create_snapshot_manager()
    
    # 测试数据
    test_jobs = [
        {
            "岗位名称": "机器学习工程师",
            "公司名称": "华为技术有限公司",
            "工作地点": "北京",
            "岗位链接": "https://example.com/job1"
        },
        {
            "岗位名称": "深度学习工程师", 
            "公司名称": "字节跳动",
            "工作地点": "北京",
            "岗位链接": "https://example.com/job2"
        }
    ]
    
    # 捕获快照
    snapshot.capture("test_input", test_jobs, {"stage": "测试输入", "source": "测试"})
    
    # 修改数据后再次快照
    filtered_jobs = [test_jobs[0]]  # 模拟去重
    snapshot.capture("test_output", filtered_jobs, {"stage": "测试输出", "filter": "去重"})
    
    # 保存摘要
    snapshot.save_summary()
    
    # 测试对比
    comparison = snapshot.compare_snapshots("test_input", "test_output")
    print(f"📊 快照对比结果: {json.dumps(comparison, ensure_ascii=False, indent=2)}")
    
    print("\n✅ 数据快照系统测试完成!")
    print("📁 查看生成的文件:")
    print("   - debug/snapshots/*_summary.json")
    print("   - debug/snapshots/*_test_input.json")
    print("   - debug/snapshots/*_test_output.json")