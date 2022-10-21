import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import random
from selenium import webdriver
from time import sleep, time
from multiprocessing import Process



def get_cookies():
    link_list = ['https://www.wayfair.com/furniture/pdp/steelside-desk-and-chair-set-tnfi2412.html','https://www.wayfair.com/brand/bnd/lark-manor-b37152.html','https://www.wayfair.com/furniture/pdp/ophelia-co-nicoletti-loveseat-with-pillow-lrkm4181.html','https://www.wayfair.com/furniture/pdp/willa-arlo-interiors-sheehan-table-with-shelf-tempered-glass-gold-w007033665.html','https://www.wayfair.com/furniture/pdp/wade-logan-jowers-tv-stand-for-tvs-up-to-65-w003381831.html','https://www.wayfair.com/outdoor/sb0/pergolas-c1828022.html','https://www.wayfair.com/outdoor/pdp/purple-leaf-13-ft-w-x-10-ft-d-aluminium-pergola-with-canopy-pule1290.html','https://www.wayfair.com/outdoor/pdp/fleur-de-lis-living-sofie-567-h-x-3543-w-steel-outdoor-fireplace-w004994717.html','https://www.wayfair.com/home-improvement/sb0/single-vanities-c531826.html','https://www.wayfair.com/furniture/pdp/hazelwood-home-charlton-vintage-upholstered-side-chair-haze2466.html']
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    # chrome_options.add_argument("--headless")
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
            ele_list = chrome.find_elements_by_class_name("nav-StoreLogo-svg")
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
    first_num = random.randint(55, 76)
    third_num = random.randint(0, 3800)
    fourth_num = random.randint(0, 140)
    os_type = [
        '(Windows NT 6.1; WOW64)', '(Windows NT 10.0; WOW64)', '(X11; Linux x86_64)',
        '(Macintosh; Intel Mac OS X 10_14_5)'
    ]
    chrome_version = 'Chrome/{}.0.{}.{}'.format(
        first_num, third_num, fourth_num)

    ua = ' '.join(['Mozilla/5.0', random.choice(os_type), 'AppleWebKit/537.36',
                   '(KHTML, like Gecko)', chrome_version, 'Safari/537.36']
                  )
    return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.42'


def get_proxy():
    # return {'proxy': '221.131.141.243:9091'}
    return requests.get("http://127.0.0.1:5010/get/").json()


def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))


def lists_combination(lists, codes='|'):
    """
    用于不同列表间的排列组合
    :param lists: 输入需要组合的列表集合（列表形式）
    :param codes: 组合间分割符号，默认为'|'
    :return: 返回列表，元素为字符串
    """
    try:
        import reduce
    except BaseException:
        from functools import reduce

    def myfunc(list1, list2):
        return [str(i) + codes + str(j) for i in list1 for j in list2]

    return reduce(myfunc, lists)


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



def get_all_combine(categories):
    combine_list = []
    cate_dict = {}
    for category in categories:
        options = category['options']
        list_by_cate = []
        for option in options:
            list_by_cate.append(option['option_id'])
            value = "{}:{}".format(option['category'], option['name'])
            cate_dict[option['option_id']] = value
        combine_list.append(list_by_cate)

    all_list = lists_combination(combine_list)
    new_all_list = []
    for combination in all_list:
        new_all_list.append(combination.split('|'))
    print('已获取 {} 个类别，共计 {} 个选择'.format(len(categories), len(cate_dict)))
    return new_all_list, cate_dict


def not_bot1(new_url, proxy, cookie):
    print(new_url)
    headers = {
        'cookie': cookie,
        'user-agent': get_ua(),
        'referer': 'https://www.wayfair.com/furniture/cat/furniture-c45974.html',
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
            e = soup.find_all('svg', class_="nav-StoreLogo-svg")
            e=e[0]
            result = 1
        except BaseException:
            print('正在绕过机器人检测')
            cookie = get_cookies()
            headers = {
                'cookie': cookie,
                'user-agent': get_ua(),
                'referer': 'https://www.wayfair.com/furniture/cat/furniture-c45974.html',
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


def not_bot2(new_url, proxy, cookie):
    print(new_url)
    headers = {
        'cookie': cookie,
        'user-agent': get_ua(),
        'referer': 'https://www.wayfair.com/furniture/cat/furniture-c45974.html',
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
            e = soup.find_all('svg', class_="nav-StoreLogo-svg")
            e=e[0]
            result = 1
        except BaseException:
            print('正在绕过机器人检测')

            cookie = get_cookies()
            headers = {
                'cookie': cookie,
                'user-agent': get_ua(),
                'referer': 'https://www.wayfair.com/furniture/cat/furniture-c45974.html',
                'upgrade-insecure-requests': '1'
            }
            proxy = get_proxy().get("proxy")
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
    return soup, proxy, cookie


def get_info(sku, c_sku, new_url, proxy, cookie):
    sp, proxy, cookie = not_bot2(new_url, proxy, cookie)
    title = sp.find_all(
        'h1', class_="pl-Heading pl-Heading--pageTitle pl-Box--defaultColor")
    title = title[0].get_text()
    salePrice = sp.find_all('span', attrs={"font-size": "5000"})
    salePrice = salePrice[0].get_text()
    button_list = sp.find_all(
        'button', class_="OutOfStockOverlay OutOfStockOverlay--withAnchor")
    if not button_list:
        is_out_of_stock = 'in_stock'
    else:
        is_out_of_stock = 'out_of_stock'

    output = [sku[0], c_sku, title, is_out_of_stock, salePrice]
    print('已获取 {} 的全部信息\n{}'.format(c_sku, output))
    return output, proxy, cookie
    # return [sku[0], c_sku, link_ava, waymore_num, is_out_of_stock,
    #         pic_num, len(link_list)] + link_list


def get_all_sku(sku, table1, proxy, cookie):
    soup, proxy, sp, cookie = not_bot1('https://www.wayfair.com/keyword.php?keyword=' +
                                       sku[0], proxy=proxy, cookie=cookie)
    try:
        text = soup.find_all('script', type='text/javascript')
        try:
            application = json.loads(text[-1].string[29:-1])["application"]
        except BaseException:
            application = json.loads(
                text[-1].string[29:-1])["temp-application"]
        prop = application["props"]
        categories = prop['options']['standardOptions']
        exception = prop['options']['exceptions']
        exceptions = []
        for i in exception:
            i.sort()
            new_i = []
            for g in i:
                new_i.append(str(g))
            exceptions.append(new_i)
        if len(categories) > 0:
            all_combination, cate_dict = get_all_combine(categories)

            # all_list = []
            url1 = json.loads(sp.headers['x-wayfair-workers-debug'])["host"]
            url2 = json.loads(sp.headers['x-wayfair-workers-debug'])["path"]
            url = 'https://' + url1 + url2
            for com in all_combination:
                com.sort()
                if com not in exceptions:
                    # all_list.append(com)
                    x = 1
                    piids = ''
                    c_sku = ''
                    for piid in com:
                        if x == 1:
                            piids = piid
                            c_sku = cate_dict[piid]
                        else:
                            piids = piids + "%2C" + piid
                            c_sku = c_sku + '&' + cate_dict[piid]
                        x += 1
                    new_url = url + "?&piid=" + piids

                    list1, proxy, cookie = get_info(
                        sku, c_sku, new_url, proxy, cookie)
                    table1.append(list1)
        else:
            url1 = json.loads(sp.headers['x-wayfair-workers-debug'])["host"]
            url2 = json.loads(sp.headers['x-wayfair-workers-debug'])["path"]
            new_url = 'https://' + url1 + url2

            list1, proxy, cookie = get_info(
                sku, sku[0], new_url, proxy, cookie)
            table1.append(list1)
    except Exception:
        c_sku = '-'
        is_out_of_stock = '-'
        title = '-'
        salePrice = '-'
        list1 = [
            sku[0],
            c_sku,
            title,
            is_out_of_stock,
            salePrice]
        table1.append(list1)
    print('已获取{}所有sku信息'.format(sku[0]))
    return table1, proxy, cookie


def process(num1, num2, n):
    csv_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_list.csv'
    data = read_src(csv_path)

    time = datetime.today().strftime("%Y%m%d")
    csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\PriceOutput_' + \
        time + '_' + n + '.csv'

    table1 = []
    cookie = get_cookies()
    for sku in data[num1:num2]:
        print("总体进度：{}/{}".format(data.index(sku), len(data)))
        proxy = '221.131.141.243:9091'
        # sp,proxy,sp1 = not_bot1('https://www.wayfair.com/keyword.php?keyword=' +
        #              sku[0],)
        # list_waymore = sp.find_all_by_xpath(
        #     '/html/body/div//section[@class="WaymoreModule"]')
        # waymore_num = len(list_waymore)
        # waymore_num='-'
        table1, proxy, cookie = get_all_sku(sku, table1, proxy, cookie)

    with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerows(table1)
    pass


def main():
    process_list = []
    p1 = Process(target=process, args=(None, 450, "1"))
    p1.start()
    p2 = Process(target=process, args=(450, 900, "2"))
    p2.start()
    p3 = Process(target=process, args=(900, 1250, "3"))
    p3.start()
    p4 = Process(target=process, args=(1250, None, "4"))
    p4.start()

    process_list.append(p1)
    process_list.append(p2)
    process_list.append(p3)
    process_list.append(p4)

    for t in process_list:
        t.join()


if __name__ == '__main__':
    s = time()
    main()
    e = time()
    print('总用时：{}'.format(str(e - s)))
