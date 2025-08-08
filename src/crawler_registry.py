"""
爬虫注册器
用于管理和注册不同的招聘网站爬虫
"""
from typing import Dict, Callable, Optional, List
import importlib
import asyncio

class CrawlerRegistry:
    """爬虫注册器，管理所有可用的爬虫"""
    
    def __init__(self):
        self.crawlers: Dict[str, Dict] = {}
        self._register_default_crawlers()
    
    def _register_default_crawlers(self):
        """注册默认的爬虫"""
        
        # Boss直聘 Playwright版本
        self.register_crawler(
            name="boss_playwright",
            display_name="Boss直聘",
            module_name="src.playwright_boss",
            function_name="fetch_boss_jobs_playwright",
            description="使用Playwright的Boss直聘爬虫，稳定性较好",
            supports_city=True,
            supports_keyword=True
        )
        
        # 可以在这里添加更多爬虫
        # self.register_crawler(
        #     name="zhilian",
        #     display_name="智联招聘", 
        #     module_name="zhilian_crawler",
        #     function_name="fetch_zhilian_jobs",
        #     description="智联招聘爬虫",
        #     supports_city=True,
        #     supports_keyword=True
        # )
    
    def register_crawler(
        self, 
        name: str, 
        display_name: str,
        module_name: str, 
        function_name: str,
        description: str = "",
        supports_city: bool = True,
        supports_keyword: bool = True
    ):
        """注册一个新的爬虫"""
        self.crawlers[name] = {
            "display_name": display_name,
            "module_name": module_name,
            "function_name": function_name,
            "description": description,
            "supports_city": supports_city,
            "supports_keyword": supports_keyword,
            "loaded": False,
            "crawler_func": None,
            "error": None
        }
    
    def load_crawler(self, name: str) -> bool:
        """动态加载指定的爬虫"""
        if name not in self.crawlers:
            print(f"❌ 未知的爬虫: {name}")
            return False
        
        crawler_info = self.crawlers[name]
        
        if crawler_info["loaded"]:
            return True
        
        try:
            # 动态导入模块
            module = importlib.import_module(crawler_info["module_name"])
            crawler_func = getattr(module, crawler_info["function_name"])
            
            crawler_info["crawler_func"] = crawler_func
            crawler_info["loaded"] = True
            crawler_info["error"] = None
            
            print(f"✅ 成功加载爬虫: {crawler_info['display_name']}")
            return True
            
        except ImportError as e:
            error_msg = f"模块导入失败: {e}"
            crawler_info["error"] = error_msg
            print(f"❌ 加载爬虫失败 {crawler_info['display_name']}: {error_msg}")
            return False
        except AttributeError as e:
            error_msg = f"函数不存在: {e}"
            crawler_info["error"] = error_msg
            print(f"❌ 加载爬虫失败 {crawler_info['display_name']}: {error_msg}")
            return False
        except Exception as e:
            error_msg = f"未知错误: {e}"
            crawler_info["error"] = error_msg
            print(f"❌ 加载爬虫失败 {crawler_info['display_name']}: {error_msg}")
            return False
    
    def get_available_crawlers(self) -> List[str]:
        """获取所有已注册的爬虫名称"""
        return list(self.crawlers.keys())
    
    def get_loaded_crawlers(self) -> Dict[str, Callable]:
        """获取所有已加载的爬虫"""
        loaded = {}
        for name, info in self.crawlers.items():
            if info["loaded"] and info["crawler_func"]:
                loaded[name] = info["crawler_func"]
        return loaded
    
    def load_enabled_crawlers(self, enabled_sites: List[str]) -> Dict[str, Callable]:
        """加载配置中启用的爬虫"""
        loaded_crawlers = {}
        
        for site in enabled_sites:
            if site in self.crawlers:
                if self.load_crawler(site):
                    crawler_info = self.crawlers[site]
                    loaded_crawlers[crawler_info["display_name"]] = crawler_info["crawler_func"]
                else:
                    print(f"⚠️  无法加载爬虫: {site}")
            else:
                print(f"⚠️  未知的爬虫配置: {site}")
                print(f"可用的爬虫: {', '.join(self.get_available_crawlers())}")
        
        return loaded_crawlers
    
    def get_crawler_info(self, name: str) -> Optional[Dict]:
        """获取爬虫信息"""
        return self.crawlers.get(name)
    
    def list_crawlers(self):
        """列出所有爬虫的详细信息"""
        print("📋 已注册的爬虫:")
        for name, info in self.crawlers.items():
            status = "✅ 已加载" if info["loaded"] else ("❌ 加载失败" if info["error"] else "⏳ 未加载")
            print(f"  • {name} ({info['display_name']})")
            print(f"    状态: {status}")
            print(f"    描述: {info['description']}")
            if info["error"]:
                print(f"    错误: {info['error']}")
            print()

# 创建全局实例
crawler_registry = CrawlerRegistry()

# 测试函数
async def test_crawler_registry():
    """测试爬虫注册器"""
    print("🧪 测试爬虫注册器...")
    
    # 列出所有爬虫
    crawler_registry.list_crawlers()
    
    # 测试加载爬虫
    enabled_sites = ["boss_playwright"]
    loaded = crawler_registry.load_enabled_crawlers(enabled_sites)
    
    print(f"📊 加载结果: {len(loaded)} 个爬虫")
    for name, func in loaded.items():
        print(f"  • {name}: {func.__name__}")

if __name__ == "__main__":
    asyncio.run(test_crawler_registry())