import yaml
import os
from typing import Dict, Any, Optional

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    加载配置文件，默认从 job_agent/config.yaml 读取
    包含更好的错误处理和默认值
    """
    if config_path is None:
        # 尝试多个可能的配置文件位置
        possible_paths = [
            os.path.join(os.path.dirname(__file__), "config.yaml"),
            os.path.join(os.getcwd(), "config.yaml"),
            os.path.join(os.getcwd(), "job_agent", "config.yaml"),
        ]
        
        config_path = None
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break
        
        if not config_path:
            print("⚠️  未找到配置文件，使用默认配置")
            return get_default_config()
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            
        # 验证配置结构
        validated_config = validate_config(config)
        print(f"✅ 成功加载配置文件: {config_path}")
        return validated_config
        
    except FileNotFoundError:
        print(f"⚠️  配置文件不存在: {config_path}，使用默认配置")
        return get_default_config()
    except yaml.YAMLError as e:
        print(f"⚠️  配置文件格式错误: {e}，使用默认配置")
        return get_default_config()
    except Exception as e:
        print(f"⚠️  配置文件加载失败: {e}，使用默认配置")
        return get_default_config()

def get_default_config() -> Dict[str, Any]:
    """
    返回默认配置
    """
    return {
        "filter": {
            "location_keywords": ["北京", "上海", "深圳", "杭州", "远程"],
            "min_salary": 25000,  # 最低月薪25K
            "max_experience": 8,   # 最多8年经验要求
            "required_keywords": ["大模型", "LLM", "机器学习", "深度学习", "AI"]
        },
        "crawler": {
            "max_pages": 3,
            "concurrent_limit": 3,
            "request_delay": 1.0,
            "max_retries": 3
        },
        "llm": {
            "model": "gpt-4o-mini",
            "temperature": 0,
            "max_tokens": 500
        }
    }

def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    验证并补全配置项
    """
    default_config = get_default_config()
    
    # 确保必要的顶级键存在
    for key in ["filter", "crawler", "llm"]:
        if key not in config:
            config[key] = default_config[key]
            print(f"⚠️  配置中缺少 '{key}' 部分，使用默认值")
    
    # 验证filter配置
    filter_config = config["filter"]
    default_filter = default_config["filter"]
    
    for key, default_value in default_filter.items():
        if key not in filter_config:
            filter_config[key] = default_value
            print(f"⚠️  筛选配置中缺少 '{key}'，使用默认值: {default_value}")
        elif key == "location_keywords" and not isinstance(filter_config[key], list):
            print(f"⚠️  location_keywords 应为列表格式，当前值: {filter_config[key]}")
            filter_config[key] = default_value
        elif key == "required_keywords" and not isinstance(filter_config[key], list):
            print(f"⚠️  required_keywords 应为列表格式，当前值: {filter_config[key]}")
            filter_config[key] = default_value
        elif key in ["min_salary", "max_experience"] and not isinstance(filter_config[key], (int, float)):
            print(f"⚠️  {key} 应为数字格式，当前值: {filter_config[key]}")
            filter_config[key] = default_value
    
    # 验证数值范围
    if filter_config["min_salary"] < 0:
        print("⚠️  最低薪资不能为负数，设置为0")
        filter_config["min_salary"] = 0
    
    if filter_config["max_experience"] < 0:
        print("⚠️  最大经验要求不能为负数，设置为0")
        filter_config["max_experience"] = 0
    
    return config

def save_config(config: Dict[str, Any], config_path: Optional[str] = None) -> bool:
    """
    保存配置到文件
    """
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, 
                     allow_unicode=True, sort_keys=False)
        
        print(f"✅ 配置已保存到: {config_path}")
        return True
        
    except Exception as e:
        print(f"❌ 配置保存失败: {e}")
        return False

# 测试函数
if __name__ == "__main__":
    print("测试配置加载...")
    config = load_config()
    print("配置内容:")
    print(yaml.dump(config, default_flow_style=False, allow_unicode=True))