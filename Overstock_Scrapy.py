import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import random
from selenium import webdriver
from time import sleep, time,strftime,gmtime
from multiprocessing import Process,Manager

# __NEXT_DATA__
def get_cookies():
    link_list = ['https://www.overstock.com/Home-Garden/Furniture/32/dept.html','https://www.overstock.com/Home-Garden/Benches/2740/subcat.html','https://www.overstock.com/c/bedding?t=1','https://www.overstock.com/c/bedding/comforters-and-sets?t=50','https://www.overstock.com/Home-Garden/Kitchen-Dining/2/dept.html','https://www.overstock.com/Home-Garden/Cabinets/18570/subcat.html','https://www.overstock.com/Home-Garden/Dining-Chairs/2022/subcat.html','https://www.overstock.com/Home-Garden/Dining-Sets/18551/subcat.html','https://www.overstock.com/Home-Garden/File-Cabinets/2792/subcat.html','https://www.overstock.com/Home-Garden/Desks/2025/subcat.html']
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome = webdriver.Chrome(r"D:\chromedriver.exe", options=chrome_options)
    n = 1
    while n<10:
        newTab = 'window.open("{}","_blank");'.format(random.choice(link_list))
        chrome.execute_script(newTab)
        windows = chrome.window_handles
        chrome.switch_to.window(windows[n])
        chrome.implicitly_wait(20)
        try:
            ele_list = chrome.find_elements_by_class_name("Logo_logoContainer_32")
            e = ele_list[0]
            n=10
        except:
            # chrome.delete_all_cookies()
            # chrome.close()
            n+=1
    # 就当成js语句吧
    sleep(3)
    cookies = chrome.get_cookies()
    final_cookies = ''
    for cookie in cookies:
        # print(cookie['name'] + '=' + cookie['value'] + '; ')
        item = cookie['name'] + '=' + cookie['value'] + '; '
        final_cookies = final_cookies + item
    final_cookies = final_cookies[:-2]

    chrome.quit()
    return final_cookies

def get_ua():
   return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.42'

def get_proxy():
    # return {'proxy': '221.131.141.243:9091'}
    return requests.get("http://127.0.0.1:5010/get/").json()

def read_src(csv_path):
    """
    用于读取指定csv文件的第一列，返回列表的元素为列表形式，已去除表头
    :param csv_path:文件地址
    :return:返回列表
    """
    data = []
    with open(csv_path, 'r', encoding='gbk') as f:
        reader = csv.reader(f, dialect='excel')
        for row in reader:
            data.append(row)
    data.pop(0)
    return data

def not_bot1(new_url, proxy, cookie):
    print(new_url)
    headers = {
        'cookie': cookie,
        'user-agent': get_ua(),
        'upgrade-insecure-requests': '1'
    }
    sp = requests.session().get(
        new_url,
        proxies={
            "http": "http://{}".format(proxy)},
        headers=headers,
        allow_redirects=False)
    while sp.status_code == 301 or sp.status_code == 302:
        url = sp.headers['Location']
        sp = requests.session().get(url, proxies={"http": "http://{}".format(proxy)}, headers=headers,
                                    allow_redirects=False)
    content = sp.content
    soup = BeautifulSoup(content, "html.parser")

    result = 0
    while result == 0:
        sleep(3)
        try:

            e = soup.find_all('a', class_="Logo_logoContainer_32")
            e=e[0]
            result = 1
        except BaseException:
            print('正在绕过机器人检测')
            cookie = get_cookies()
            headers = {
                'cookie': cookie,
                'user-agent': get_ua(),
                'upgrade-insecure-requests': '1'
            }
            proxy = get_proxy().get("proxy")

            sp = requests.session().get(new_url, proxies={"http": "http://{}".format(proxy)}, headers=headers,
                                        allow_redirects=False)
            while sp.status_code == 301 or sp.status_code == 302:
                url = sp.headers['Location']

                sp = requests.session().get(url, proxies={"http": "http://{}".format(proxy)}, headers=headers,
                                            allow_redirects=False)

            content = sp.content
            soup = BeautifulSoup(content, "html.parser")
            result = 0
    return soup, proxy, sp, cookie

def get_info(link, table1,proxy, cookie):
    soup, proxy, sp, cookie = not_bot1(link, proxy, cookie)
    try:
        text = soup.find_all('script', id='__NEXT_DATA__')
        options = json.loads(text[0])['props']['pageProps']['product']['options']
        for option in options:
            price = option['price']
            subSku = option['subSku']
            decription = option['decription']
            qtyOnHand = option['qtyOnHand']
            output = [link,subSku,decription,price,qtyOnHand]
            table1.append(output)
    except Exception as e:
        print(e)
        pass
    print('已获取{}全部信息'.format(link))
    return table1, proxy, cookie

def process(num1, num2, table1):
    csv_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\SKU_list.csv'
    data = read_src(csv_path)
    cookie = get_cookies()
    for link in data[num1:num2]:
        print("总体进度：{}/{}".format(data.index(link), len(data)))
        link = link[0]
        proxy = get_proxy()
        table1, proxy, cookie = get_info(link, table1, proxy, cookie)

    return table1

def main():
    time = datetime.today().strftime("%Y%m%d")
    process_list = []
    manager = Manager()
    table1 = manager.list()
    p1 = Process(target=process, args=(None, 300, table1))
    p1.start()
    p2 = Process(target=process, args=(300, 600, table1))
    p2.start()
    p3 = Process(target=process, args=(600, 900, table1))
    p3.start()
    p4 = Process(target=process, args=(900, None, table1))
    p4.start()

    process_list.append(p1)
    process_list.append(p2)
    process_list.append(p3)
    process_list.append(p4)

    for t in process_list:
        t.join()

    csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\PriceOutput_' + \
        time + '.csv'
    with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerows(table1)
    pass

if __name__ == '__main__':
    s = time()
    main()
    e = time()
    print('总用时：{}s'.format(strftime("%H:%M:%S", gmtime(e - s))))