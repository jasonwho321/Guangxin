from selenium import webdriver
from msedge.selenium_tools import Edge, EdgeOptions
from time import sleep
chrome_options = webdriver.ChromeOptions() # 代理IP,由快代理提供
chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome = webdriver.Chrome(r"D:\chromedriver.exe",options=chrome_options)
chrome.get('https://www.wayfair.com/furniture/pdp/hazelwood-home-charlton-vintage-upholstered-side-chair-haze2466.html')
sleep(3)
newTab = 'window.open("https://www.wayfair.com/furniture/pdp/hazelwood-home-charlton-vintage-upholstered-side-chair-haze2466.html");' #就当成js语句吧
chrome.execute_script(newTab) #输出js语句
cookies = chrome.get_cookies()
final_cookies = ''
for cookie in cookies:
    item = cookie['name']+'='+cookie['value']+'; '
    final_cookies = final_cookies+item
final_cookies = final_cookies[:-2]

# chrome.delete_all_cookies()
# chrome.
# chrome.get('https://www.wayfair.com/furniture/pdp/hazelwood-home-charlton-vintage-upholstered-side-chair-haze2466.html')
# print(chrome.page_source)
# 百度查IP chrome.get('https://www.baidu.com/s?ie=UTF-8&wd=ip') print(chrome.page_source)
# chrome.quit() #退出


# options = EdgeOptions()
#
# options.add_experimental_option('excludeSwitches', ['enable-automation'])
# options.add_argument("--disable-blink-features=AutomationControlled")
# options.add_experimental_option('useAutomationExtension', False)
#
#
#
# options.use_chromium = True
# # options.add_argument("--proxy-server=http://101.68.58.135:8085")
# options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.34")
#
# driver = Edge(executable_path=r"D:\msedgedriver.exe", options=options)
# driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
#    'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
# })

from time import sleep
# 设置代理


# 查看本机ip，查看代理是否起作用
# driver.get("http://httpbin.org/ip")

# driver.get('https://www.wayfair.com')
# driver.delete_all_cookies()
# driver.get('https://www.wayfair.com/furniture/pdp/hazelwood-home-charlton-vintage-upholstered-side-chair-haze2466.html')
# url = 'https://www.wayfair.com/furniture/pdp/williston-forge-hartmann-counter-bar-stool-w004382256.html'
# for i in range(1000):
# driver.get(url)
# driver.close()
# print(driver.page_source)

# 退出，清除浏览器缓存
