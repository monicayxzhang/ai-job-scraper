import asyncio
import json
import random
import time
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, Browser
import os

async def setup_page(page: Page) -> None:
    """配置页面以避免检测"""
    # 设置用户代理 - 修复API调用
    await page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    
    # 设置视口
    await page.set_viewport_size({"width": 1920, "height": 1080})
    
    # 添加一些反检测脚本
    await page.add_init_script("""
        // 覆盖webdriver属性
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });
        
        // 覆盖plugins长度
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
        
        // 覆盖语言
        Object.defineProperty(navigator, 'languages', {
            get: () => ['zh-CN', 'zh', 'en'],
        });
        
        // 覆盖权限
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
    """)

async def human_like_delay():
    """模拟人类操作的随机延迟"""
    await asyncio.sleep(random.uniform(1.5, 3.5))

async def scroll_page(page: Page):
    """模拟人类滚动行为"""
    # 随机滚动
    for _ in range(random.randint(2, 5)):
        await page.mouse.wheel(0, random.randint(200, 800))
        await asyncio.sleep(random.uniform(0.5, 1.5))

async def extract_job_info_from_page(page: Page) -> List[Dict[str, str]]:
    """从页面提取岗位信息"""
    jobs = []
    
    try:
        # 等待岗位列表加载 - 修复选择器
        await page.wait_for_selector('.rec-job-list', timeout=10000)
        
        # 提取岗位信息 - 使用正确的选择器
        job_elements = await page.query_selector_all('.job-card-wrap')
        
        print(f"📄 页面找到 {len(job_elements)} 个岗位元素")
        
        for job_element in job_elements:
            try:
                # 提取基本信息 - 根据实际HTML结构调整
                job_name_elem = await job_element.query_selector('.job-name')
                company_name_elem = await job_element.query_selector('.boss-name')
                salary_elem = await job_element.query_selector('.job-salary')
                location_elem = await job_element.query_selector('.company-location')
                tag_elements = await job_element.query_selector_all('.tag-list li')
                
                # 获取岗位链接
                job_url = ""
                if job_name_elem:
                    job_url = await job_name_elem.get_attribute('href')
                    if job_url and not job_url.startswith('http'):
                        job_url = f"https://www.zhipin.com{job_url}"
                
                # 提取文本内容
                job_name = await job_name_elem.inner_text() if job_name_elem else ""
                company_name = await company_name_elem.inner_text() if company_name_elem else ""
                salary = await salary_elem.inner_text() if salary_elem else ""
                location = await location_elem.inner_text() if location_elem else ""
                
                # 提取标签信息（经验要求、学历等）
                tags = []
                for tag_elem in tag_elements:
                    tag_text = await tag_elem.inner_text()
                    if tag_text:
                        tags.append(tag_text.strip())
                
                if job_name and company_name:  # 确保有基本信息
                    job_info = {
                        "job_name": job_name.strip(),
                        "company_name": company_name.strip(),
                        "salary_desc": salary.strip(),
                        "location": location.strip(),
                        "tags": ", ".join(tags),
                        "url": job_url
                    }
                    jobs.append(job_info)
                    
            except Exception as e:
                print(f"⚠️  提取单个岗位信息失败: {e}")
                continue
                
    except Exception as e:
        print(f"❌ 提取岗位信息失败: {e}")
    
    return jobs

async def get_job_detail_html(page: Page, job_url: str) -> Optional[str]:
    """获取岗位详情页HTML"""
    try:
        print(f"🔍 访问岗位详情: {job_url}")
        
        # 降低等待要求，增加超时时间
        await page.goto(job_url, wait_until='domcontentloaded', timeout=45000)
        await human_like_delay()
        
        # 等待页面加载，使用更宽松的条件
        try:
            await page.wait_for_selector('.job-detail', timeout=5000)
        except:
            # 如果特定元素加载失败，仍然尝试获取页面内容
            print("⚠️  岗位详情元素加载超时，尝试获取页面内容")
        
        # 获取页面HTML
        html = await page.content()
        return html
        
    except Exception as e:
        print(f"⚠️  获取详情页失败 {job_url}: {e}")
        return None

async def fetch_boss_jobs_playwright(keyword: str, city: str = "101010100", max_pages: int = 2, max_jobs_test: Optional[int] = None) -> List[Dict[str, str]]:
    """使用Playwright从Boss直聘获取岗位信息
        
    Args:
        keyword: 搜索关键词
        city: 城市代码
        max_pages: 最大页数
        max_jobs_test: 测试模式下每页最多处理的岗位数量，None表示处理所有岗位
    """
    print(f"🚀 启动Playwright爬虫: {keyword}")
    if max_jobs_test:
        print(f"🧪 测试模式: 每页最多处理 {max_jobs_test} 个岗位")
    
    # 创建保存目录
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_file = f"data/raw_boss_playwright_{timestamp}.jsonl"
    
    jobs = []
    
    async with async_playwright() as p:
        # 启动浏览器（使用Chromium）
        browser = await p.chromium.launch(
            headless=False,  # 设为True可以无界面运行
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-infobars',
                '--disable-extensions',
                '--disable-dev-shm-usage',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
        )
        
        try:
            # 创建页面
            page = await browser.new_page()
            await setup_page(page)
            
            # 构造搜索URL
            from urllib.parse import quote
            encoded_keyword = quote(keyword)
            base_url = f"https://www.zhipin.com/web/geek/job?query={encoded_keyword}&city={city}"
            
            for page_num in range(1, max_pages + 1):
                print(f"\n🔍 正在爬取第 {page_num} 页...")
                
                # 访问搜索页面
                search_url = f"{base_url}&page={page_num}"
                print(f"访问URL: {search_url}")
                
                try:
                    # 增加超时时间，降低等待标准
                    await page.goto(search_url, wait_until='domcontentloaded', timeout=60000)
                    print("✅ 页面加载成功")
                except Exception as e:
                    print(f"⚠️  页面加载失败，尝试重新加载: {e}")
                    try:
                        # 再次尝试，使用更宽松的等待条件
                        await page.goto(search_url, wait_until='load', timeout=45000)
                        print("✅ 重新加载成功")
                    except Exception as e2:
                        print(f"❌ 重新加载也失败: {e2}")
                        # 保存错误页面
                        error_file = f"data/error_page_{page_num}_{timestamp}.html"
                        try:
                            content = await page.content()
                            with open(error_file, "w", encoding="utf-8") as f:
                                f.write(content)
                            print(f"已保存错误页面到: {error_file}")
                        except:
                            pass
                        continue
                
                await human_like_delay()
                
                # 检查是否有反爬虫页面
                page_title = await page.title()
                page_content = await page.content()
                
                if "异常" in page_title or "验证" in page_content or "加载中" in page_content:
                    print("⚠️  遇到反爬虫页面，尝试等待...")
                    await asyncio.sleep(10)
                    
                    # 尝试刷新
                    await page.reload(wait_until='networkidle')
                    await human_like_delay()
                
                # 模拟人类行为
                await scroll_page(page)
                await human_like_delay()
                
                # 提取当前页面的岗位信息
                page_jobs = await extract_job_info_from_page(page)
                
                if not page_jobs:
                    print(f"⚠️  第 {page_num} 页没有找到岗位信息")
                    
                    # 保存调试信息
                    debug_file = f"data/debug_playwright_page_{page_num}_{timestamp}.html"
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(await page.content())
                    print(f"已保存调试页面到: {debug_file}")
                    continue

                # 应用测试模式限制
                if max_jobs_test and len(page_jobs) > max_jobs_test:
                    page_jobs = page_jobs[:max_jobs_test]
                    print(f"🧪 测试模式: 限制为前 {max_jobs_test} 个岗位")
                
                print(f"✅ 第 {page_num} 页提取到 {len(page_jobs)} 个岗位")
                
                # 处理每个岗位
                for i, job_info in enumerate(page_jobs):
                    print(f"处理岗位 {i+1}/{len(page_jobs)}: {job_info['job_name']}")
                    
                    # 获取详情页HTML（可选，耗时较长）
                    job_html = ""
                    if job_info.get("url"):
                        # 在测试模式下，获取所有岗位的详情页（因为数量已经被限制了）
                        job_html = await get_job_detail_html(page, job_info["url"])
                        await human_like_delay()
                    
                    # 保存岗位数据
                    job_record = {
                        "url": job_info.get("url", ""),
                        "html": job_html,
                        "api_data": job_info,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "Boss直聘Playwright"
                    }
                    
                    jobs.append(job_record)
                    
                    # 实时保存
                    with open(raw_file, "a", encoding="utf-8") as f:
                        f.write(json.dumps(job_record, ensure_ascii=False) + "\n")
                    
                    print(f"✅ 保存: {job_info['job_name']} - {job_info['company_name']}")
                
                # 页面间延迟
                print(f"⏳ 等待后继续下一页...")
                await asyncio.sleep(random.uniform(3, 6))
        
        finally:
            await browser.close()
    
    print(f"\n🎉 爬取完成！")
    print(f"💾 数据已保存到: {raw_file}")
    print(f"📊 总共获取 {len(jobs)} 个岗位")
    
    return jobs

# 测试函数
if __name__ == "__main__":
    async def test():
        try:
            results = await fetch_boss_jobs_playwright(
                keyword="大模型 算法", 
                city="101010100",  # 北京
                max_pages=1
            )
            
            print(f"\n📋 岗位预览:")
            for i, job in enumerate(results[:5]):
                api_data = job.get("api_data", {})
                print(f"{i+1}. {api_data.get('job_name', 'N/A')} - {api_data.get('company_name', 'N/A')}")
                print(f"   薪资: {api_data.get('salary_desc', 'N/A')}")
                print(f"   地点: {api_data.get('location', 'N/A')}")
                print(f"   URL: {job.get('url', 'N/A')}")
                print()
                
        except Exception as e:
            print(f"❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()
    
    asyncio.run(test())