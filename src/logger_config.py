# logger_config.py - 统一日志管理系统（修复版）
"""
🔧 修复版: 新增无数据情况的日志方法，区分"系统错误"和"无新数据"
"""
import logging
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
import sys

class LogLevel(Enum):
    PRODUCTION = "production"    # 最简洁
    NORMAL = "normal"           # 标准信息
    DEBUG = "debug"             # 详细调试
    TRACE = "trace"             # 最详细

class JobAgentLogger:
    def __init__(self, level: LogLevel = LogLevel.NORMAL, save_debug_data: bool = True):
        self.level = level
        self.save_debug_data = save_debug_data
        self.debug_data: List[Dict[str, Any]] = []
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 确保调试目录存在
        os.makedirs("debug", exist_ok=True)
        os.makedirs("debug/snapshots", exist_ok=True)
        
        # 设置Python日志
        self._setup_python_logging()
        
        # 初始化会话
        self._log_session_start()
    
    def _setup_python_logging(self):
        """设置Python标准日志"""
        # 创建logger
        self.python_logger = logging.getLogger(f"job_agent_{self.session_id}")
        self.python_logger.setLevel(logging.DEBUG)
        
        # 清除已有的handlers
        self.python_logger.handlers.clear()
        
        # 文件handler
        log_file = f'debug/pipeline_{self.session_id}.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # 控制台handler - 根据日志级别调整
        console_handler = logging.StreamHandler(sys.stdout)
        if self.level == LogLevel.PRODUCTION:
            console_handler.setLevel(logging.WARNING)
        elif self.level == LogLevel.NORMAL:
            console_handler.setLevel(logging.INFO)
        else:
            console_handler.setLevel(logging.DEBUG)
        
        # 格式化
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加handlers
        self.python_logger.addHandler(file_handler)
        self.python_logger.addHandler(console_handler)
        
        print(f"[LOG] Log file: {log_file}")
    
    def _log_session_start(self):
        """记录会话开始"""
        session_info = {
            "session_id": self.session_id,
            "log_level": self.level.value,
            "debug_data_enabled": self.save_debug_data,
            "start_time": datetime.now().isoformat()
        }
        self.info("Log system initialized", session_info)
    
    def info(self, message: str, data: Optional[Dict] = None):
        """标准信息日志"""
        if self.level != LogLevel.PRODUCTION:
            print(f"[INFO] {message}")
        
        self.python_logger.info(message)
        
        if data and self.level in [LogLevel.DEBUG, LogLevel.TRACE]:
            self._log_data("INFO", message, data)
    
    def success(self, message: str, data: Optional[Dict] = None):
        """成功信息"""
        if self.level != LogLevel.PRODUCTION:
            print(f"[SUCCESS] {message}")
        
        self.python_logger.info(f"SUCCESS: {message}")
        
        if data and self.level in [LogLevel.DEBUG, LogLevel.TRACE]:
            self._log_data("SUCCESS", message, data)
    
    # 🆕 修复版: 新增无数据情况的日志方法
    def success_no_data(self, message: str, data: Optional[Dict] = None):
        """无数据但成功的日志"""
        if self.level != LogLevel.PRODUCTION:
            print(f"[SUCCESS] {message} (no new data)")
        
        self.python_logger.info(f"SUCCESS_NO_DATA: {message}")
        
        if data and self.level in [LogLevel.DEBUG, LogLevel.TRACE]:
            self._log_data("SUCCESS_NO_DATA", message, data)
    
    def info_no_data(self, message: str, data: Optional[Dict] = None):
        """无数据情况的信息日志"""
        if self.level != LogLevel.PRODUCTION:
            print(f"[INFO] {message}")
        
        self.python_logger.info(f"NO_DATA: {message}")
        
        if data and self.level in [LogLevel.DEBUG, LogLevel.TRACE]:
            self._log_data("NO_DATA", message, data)
    
    def info_skip(self, message: str, data: Optional[Dict] = None):
        """跳过步骤的信息日志"""
        if self.level != LogLevel.PRODUCTION:
            print(f"[SKIP] {message}")
        
        self.python_logger.info(f"SKIP: {message}")
        
        if data and self.level in [LogLevel.DEBUG, LogLevel.TRACE]:
            self._log_data("SKIP", message, data)
    
    def warning(self, message: str, data: Optional[Dict] = None):
        """警告信息"""
        print(f"[WARNING] {message}")
        self.python_logger.warning(message)
        
        if data:
            self._log_data("WARNING", message, data)
    
    def error(self, message: str, data: Optional[Dict] = None, exception: Optional[Exception] = None):
        """错误信息"""
        print(f"[ERROR] {message}")
        self.python_logger.error(message)
        
        error_data = data or {}
        if exception:
            error_data.update({
                "exception_type": type(exception).__name__,
                "exception_message": str(exception)
            })
        
        self._log_data("ERROR", message, error_data)
    
    def debug(self, message: str, data: Optional[Dict] = None):
        """调试信息"""
        if self.level in [LogLevel.DEBUG, LogLevel.TRACE]:
            print(f"🔍 {message}")
            self.python_logger.debug(message)
            
            if data:
                self._log_data("DEBUG", message, data)
    
    def trace(self, message: str, data: Optional[Dict] = None):
        """最详细追踪"""
        if self.level == LogLevel.TRACE:
            print(f"🔎 {message}")
            self.python_logger.debug(f"TRACE: {message}")
            
            if data:
                self._log_data("TRACE", message, data)
    
    def _log_data(self, level: str, message: str, data: Dict):
        """保存结构化数据"""
        if self.save_debug_data:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "level": level,
                "message": message,
                "data": data
            }
            self.debug_data.append(log_entry)
    
    def step_start(self, step_name: str, step_num: int, total_steps: int):
        """步骤开始"""
        separator = "=" * 60
        step_header = f"🚀 步骤{step_num}/{total_steps}: {step_name}"
        
        if self.level != LogLevel.PRODUCTION:
            print(f"\n{separator}")
            print(step_header)
            print(separator)
        
        self.python_logger.info(f"STEP_START: {step_header}")
        
        self._log_data("STEP_START", f"步骤{step_num}: {step_name}", {
            "step_number": step_num,
            "total_steps": total_steps,
            "step_name": step_name
        })
    
    def step_end(self, step_name: str, success: bool, stats: Optional[Dict] = None):
        """步骤结束（修复版）"""
        # 🔧 修复: 根据stats内容判断是否为无数据情况
        stats = stats or {}
        
        # 检查是否为无数据的成功情况
        is_no_data_success = (
            success and 
            any(indicator in stats.get("状态", "").lower() for indicator in ["无新数据", "跳过", "无数据"]) or
            any(indicator in stats.get("原因", "").lower() for indicator in ["无新数据", "跳过", "无数据"])
        )
        
        if is_no_data_success:
            # 无数据的成功情况
            status = "ℹ️  跳过"
            step_result = f"📊 步骤完成: {step_name} - {status}"
            log_type = "STEP_END_NO_DATA"
        elif success:
            # 正常成功
            status = "✅ 成功"
            step_result = f"📊 步骤完成: {step_name} - {status}"
            log_type = "STEP_END_SUCCESS"
        else:
            # 真正的失败
            status = "❌ 失败"
            step_result = f"📊 步骤完成: {step_name} - {status}"
            log_type = "STEP_END_FAILURE"
        
        if self.level != LogLevel.PRODUCTION:
            print(f"\n{step_result}")
            if stats:
                for key, value in stats.items():
                    if key not in ["状态", "原因"]:  # 避免重复显示状态信息
                        print(f"   {key}: {value}")
        
        self.python_logger.info(f"{log_type}: {step_result}")
        
        step_data = {
            "step_name": step_name,
            "success": success,
            "is_no_data": is_no_data_success,
            "stats": stats or {}
        }
        self._log_data(log_type, f"步骤完成: {step_name}", step_data)
    
    def save_debug_session(self):
        """保存调试会话数据"""
        if self.debug_data and self.save_debug_data:
            debug_file = f"debug/debug_session_{self.session_id}.json"
            try:
                with open(debug_file, 'w', encoding='utf-8') as f:
                    json.dump(self.debug_data, f, ensure_ascii=False, indent=2)
                
                self.info(f"调试数据已保存: {debug_file}", {
                    "debug_entries": len(self.debug_data),
                    "file_path": debug_file
                })
                
                # 创建最新文件的软链接（便于快速访问）
                latest_link = "debug/debug_session_latest.json"
                if os.path.exists(latest_link):
                    os.remove(latest_link)
                
                # Windows和Linux兼容的方式
                try:
                    os.symlink(os.path.basename(debug_file), latest_link)
                except (OSError, NotImplementedError):
                    # Windows可能不支持symlink，直接复制
                    import shutil
                    shutil.copy2(debug_file, latest_link)
                
            except Exception as e:
                self.error("保存调试数据失败", {"error": str(e)}, e)
        
        # 记录会话结束
        self._log_session_end()
    
    def _log_session_end(self):
        """记录会话结束"""
        session_summary = {
            "session_id": self.session_id,
            "end_time": datetime.now().isoformat(),
            "debug_entries_count": len(self.debug_data),
            "log_level": self.level.value
        }
        self.info("日志会话结束", session_summary)

# 全局日志实例
_global_logger: Optional[JobAgentLogger] = None

def init_logger(level: LogLevel = LogLevel.NORMAL, save_debug_data: bool = True) -> JobAgentLogger:
    """初始化全局日志器"""
    global _global_logger
    _global_logger = JobAgentLogger(level, save_debug_data)
    return _global_logger

def get_logger() -> JobAgentLogger:
    """获取全局日志器"""
    global _global_logger
    if _global_logger is None:
        _global_logger = JobAgentLogger()
    return _global_logger

def cleanup_logger():
    """清理日志器（在程序结束时调用）"""
    global _global_logger
    if _global_logger:
        _global_logger.save_debug_session()
        _global_logger = None

# 装饰器：自动记录函数执行
def log_function_call(message: str = None):
    """装饰器：自动记录函数调用"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger()
            func_name = func.__name__
            log_message = message or f"调用函数: {func_name}"
            
            logger.trace(f"开始{log_message}", {
                "function": func_name,
                "args_count": len(args),
                "kwargs_keys": list(kwargs.keys())
            })
            
            try:
                result = func(*args, **kwargs)
                logger.trace(f"完成{log_message}", {
                    "function": func_name,
                    "success": True
                })
                return result
            except Exception as e:
                logger.error(f"失败{log_message}", {
                    "function": func_name,
                    "error": str(e)
                }, e)
                raise
        
        return wrapper
    return decorator

# 使用示例和测试
if __name__ == "__main__":
    # 测试不同日志级别
    print("🧪 测试修复版日志系统...")
    
    # 初始化日志器
    logger = init_logger(LogLevel.TRACE, True)
    
    # 测试不同类型的日志
    logger.step_start("测试步骤", 1, 3)
    
    logger.info("这是一条信息", {"test_data": "info_value"})
    logger.success("这是成功信息", {"result": "success"})
    logger.warning("这是警告信息", {"warning_type": "test"})
    logger.debug("这是调试信息", {"debug_level": "high"})
    logger.trace("这是追踪信息", {"trace_detail": "very_detailed"})
    
    # 🆕 测试新增的日志方法
    logger.success_no_data("处理完成但无新数据", {"processed": 0})
    logger.info_no_data("没有找到符合条件的数据", {"filter_criteria": "test"})
    logger.info_skip("跳过此步骤", {"reason": "无数据需要处理"})
    
    # 测试修复后的step_end方法
    logger.step_end("测试步骤", True, {"processed": 5, "success_rate": "100%"})
    
    # 🔧 测试无数据的成功情况
    logger.step_end("无数据步骤", True, {
        "状态": "跳过(无新数据)",
        "原因": "所有岗位已存在于数据库",
        "建议": "重新爬取获取新数据"
    })
    
    # 测试真正的失败情况
    logger.step_end("失败步骤", False, {"错误": "系统配置问题"})
    
    # 测试错误日志
    try:
        raise ValueError("这是一个测试错误")
    except Exception as e:
        logger.error("捕获到错误", {"error_context": "testing"}, e)
    
    # 保存会话
    logger.save_debug_session()
    
    print("\n[SUCCESS] Fixed logging system test complete!")
    print("📁 查看生成的文件:")
    print("   - debug/pipeline_*.log")
    print("   - debug/debug_session_*.json")
    print("   - debug/debug_session_latest.json")
    print("\n🔧 修复效果:")
    print("   [SUCCESS] Distinguished 'system error' and 'no new data'")
    print("   [SUCCESS] No data cases show as 'skipped' not 'failed'")
    print("   [SUCCESS] Provides more accurate status information")