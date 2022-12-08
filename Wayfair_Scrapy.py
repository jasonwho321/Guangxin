import os

import sys

import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import random
from selenium import webdriver
from time import sleep, time, strftime, gmtime
from multiprocessing import Manager, Pool, cpu_count
from tqdm import tqdm
from base import bot_push_text
import pandas as pd


referer_US = 'https://www.wayfair.com/furniture/cat/furniture-c45974.html'
referer_CA = 'https://www.wayfair.ca/v/global_help/global_help_app/index'
link_list_US = ['https://www.wayfair.com/furniture/pdp/steelside-desk-and-chair-set-tnfi2412.html',
                'https://www.wayfair.com/brand/bnd/lark-manor-b37152.html',
                'https://www.wayfair.com/furniture/pdp/ophelia-co-nicoletti-loveseat-with-pillow-lrkm4181.html',
                'https://www.wayfair.com/furniture/pdp/willa-arlo-interiors-sheehan-table-with-shelf-tempered-glass-gold-w007033665.html',
                'https://www.wayfair.com/furniture/pdp/wade-logan-jowers-tv-stand-for-tvs-up-to-65-w003381831.html',
                'https://www.wayfair.com/outdoor/sb0/pergolas-c1828022.html',
                'https://www.wayfair.com/outdoor/pdp/purple-leaf-13-ft-w-x-10-ft-d-aluminium-pergola-with-canopy-pule1290.html',
                'https://www.wayfair.com/outdoor/pdp/fleur-de-lis-living-sofie-567-h-x-3543-w-steel-outdoor-fireplace-w004994717.html',
                'https://www.wayfair.com/home-improvement/sb0/single-vanities-c531826.html',
                'https://www.wayfair.com/furniture/pdp/hazelwood-home-charlton-vintage-upholstered-side-chair-haze2466.html']
link_list_CA = ['https://www.wayfair.ca/furniture/pdp/ivy-bronx-paradise-pillow-top-premium-faux-leather-sofa-couch-for-home-office-living-room-grey-c004333311.html',
                'https://www.wayfair.ca/home-improvement/pdp/goo-ki-7-916-centre-to-centre-nordic-modern-simplicity-bar-pullset-of-6-gssc1046.html?categoryid=434268&placement=1&slot=0&sponsoredid=cf425645f1845ad2440383962bb942e49d6e88cc242439e7e508703f31481f88&_txid=I%2BF9OmNrFliufV30T6bfAg%3D%3D&isB2b=0&auctionId=b387ca37-fc31-41b2-baba-20dd1472fd33',
                'https://www.wayfair.ca/daily-sales/closeout',
                'https://www.wayfair.ca/furniture/sb0/sofas-c413892.html',
                'https://www.wayfair.ca/furniture/sb0/living-room-sets-c46145.html',
                'https://www.wayfair.ca/furniture/sb0/office-chairs-c478390.html',
                'https://www.wayfair.ca/furniture/sb0/office-stools-c1780288.html',
                'https://www.wayfair.ca/furniture/pdp/orren-ellis-gallardo-height-adjustable-lab-stool-c001643497.html',
                'https://www.wayfair.ca/furniture/pdp/inbox-zero-round-rolling-pu-leather-height-adjustable-lab-stool-c003137927.html',
                'https://www.wayfair.ca/furniture/sb0/office-chair-accessories-c1783512.html']
csv_path_CA = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_list_CA.csv'
csv_path_US = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_list_US.csv'
ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.42'

def mapping_sku(csv_priceout, csv_map,full_sku,partner_number):
    df_price = pd.read_csv(csv_priceout)
    df_map = pd.read_csv(csv_map)
    df_map = df_map.to_dict('list')
    df_map = dict(zip(df_map[full_sku], df_map[partner_number]))
    df_price[full_sku] = df_price[full_sku].str.strip()
    df_price[partner_number] = df_price[full_sku].map(df_map, na_action=None)
    df_price.to_csv(csv_priceout)
    return df_price

def get_cookies(country):

    link_list = link_list_US if country == "US" else link_list_CA
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    # chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome = webdriver.Chrome(r"D:\chromedriver.exe", options=chrome_options)
    n = 1
    while n < 10:
        newTab = 'window.open("{}","_blank");'.format(random.choice(link_list))
        chrome.execute_script(newTab)
        windows = chrome.window_handles
        chrome.switch_to.window(windows[n])
        chrome.implicitly_wait(20)
        if chrome.current_url.startswith(
                'https://www.wayfair.com/v/captcha/show?goto') or chrome.current_url.startswith('https://www.wayfair.ca/v/captcha/show?goto'):
            n += 1
        else:
            break
    sleep(3)
    cookies = chrome.get_cookies()
    final_cookies = ''
    for cookie in cookies:
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
    return ua


def get_proxy():
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
    return new_all_list, cate_dict


def not_bot(new_url, cookie_pool, country, lock):
    referer = referer_US if country == "US" else referer_CA
    if cookie_pool:
        cookie = random.choice(cookie_pool)
    else:
        cookie = get_cookies(country)
        cookie_pool.append(cookie)
    proxy = get_proxy().get("proxy")
    headers = {
        'cookie': cookie,
        'user-agent': ua,
        'referer': referer,
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
        if sp.url.startswith('https://www.wayfair.com/v/captcha/show?goto') or sp.url.startswith(
                'https://www.wayfair.ca/v/captcha/show?goto'):
            try:
                cookie_pool.remove(cookie)
                # delete_proxy(proxy)
            except:
                pass
            if cookie_pool:
                cookie = random.choice(cookie_pool)
            else:
                cookie = get_cookies(country)
                cookie_pool.append(cookie)
            headers = {
                'cookie': cookie,
                'user-agent': ua,
                'referer': referer,
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
        else:
            break
    return soup, sp

def get_info(sku, c_sku, new_url, cookie_pool, country, lock):
    sp, soup = not_bot(new_url, cookie_pool, country, lock)
    sale = sp.find('div', class_="ProductDetailImageCarousel-top").find_all('span', class_="Flag__StyledFlagContent-scqam4-1 kkUOYD pl-CardFlag-content")
    if sale:
        sale_tag = 'sale'
    else:
        sale_tag = '-'
    title = sp.find_all(
        'h1', class_="pl-Heading pl-Heading--pageTitle pl-Box--defaultColor")
    title = title[0].get_text()
    rating = sp.find_all(
        'span', class_="ProductRatingNumberWithCount-rating")
    rating = rating[0].get_text()
    review = sp.find_all(
        'span', class_="ProductRatingNumberWithCount-count ProductRatingNumberWithCount-count--link")
    review = review[0].get_text()
    salePrice = sp.find_all('span', attrs={"font-size": "5000"})
    salePrice = salePrice[0].get_text()
    button_list = sp.find_all(
        'button', class_="OutOfStockOverlay OutOfStockOverlay--withAnchor")
    if not button_list:
        is_out_of_stock = 'in_stock'
    else:
        is_out_of_stock = 'out_of_stock'

    output = [sku[0], c_sku,'{} {}'.format(sku[0],c_sku), title, is_out_of_stock, salePrice, rating, review]
    return output


def get_all_sku(sku, table1, cookie_pool, country, lock):
    com = 'com' if country == "US" else 'ca'
    soup, sp = not_bot('https://www.wayfair.' + com + '/keyword.php?keyword=' +
                       sku[0], cookie_pool=cookie_pool, country=country, lock=lock)
    text = soup.find_all('script', type='text/javascript')
    if "application" in dict(json.loads(text[-1].string[29:-1])):
        application = json.loads(text[-1].string[29:-1])["application"]
    else:
        application = json.loads(
            text[-1].string[29:-1])["temp-application"]


    prop = application["props"]
    try:
        flagServiceFlagData = prop["mainCarousel"]["flagServiceFlagData"]
    except:
        flagServiceFlagData = []
    categories = prop['options']['standardOptions']
    exception = prop['options']['exceptions']
    exceptions = []
    flag_list = []
    if flagServiceFlagData:
        for i in flagServiceFlagData:
            if flagServiceFlagData[i]['flagText'] == 'Sale':
                flag_list.append(i.replace('_','%2C'))
    else:
        sale_tag = '-'
    for i in exception:
        i.sort()
        new_i = []
        for g in i:
            new_i.append(str(g))
        exceptions.append(new_i)
    if len(categories) > 0:
        all_combination, cate_dict = get_all_combine(categories)
        url = sp.url
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
                list1 = get_info(
                    sku, c_sku, new_url, cookie_pool, country, lock)
                if piids in flag_list:
                    list1.append('Sale')
                else:
                    list1.append('-')
                table1.append(list1)
    else:
        new_url = sp.url
        list1 = get_info(
            sku, sku[0], new_url, cookie_pool, country, lock)
        list1.append(sale_tag)
        table1.append(list1)
    return table1


def process(sku, table1, dict1, lock, cookie_pool):
    try:
        table1 = get_all_sku(
            sku, table1, cookie_pool, dict1['country'], lock)
    except BaseException as e:
        c_sku = '-'
        is_out_of_stock = '-'
        title = '-'
        salePrice = '-'
        list1 = [
            sku[0],
            c_sku,
            '-',
            title,
            is_out_of_stock,
            salePrice, '-', '-','-']
        table1.append(list1)
    return table1


if __name__ == '__main__':
    country_list = ["US", "CA"]
    s = time()
    for country in country_list:
        proxy = '221.131.141.243:9091'
        csv_path = csv_path_US if country == "US" else csv_path_CA
        data = read_src(csv_path)
        lenth = len(data)
        date = datetime.today().strftime("%Y%m%d")

        manager = Manager()
        lock = manager.Lock()
        dict1 = manager.dict()
        dict1['country'] = country
        table1 = manager.list()
        pool_num = cpu_count()
        cookie_pool = manager.list()

        for i in range(6):
            cookie_pool.append(get_cookies(country))

        pbar = tqdm(total=lenth)
        update = lambda *args: pbar.update(1)
        workers = Pool(pool_num)
        for sku in data:
            workers.apply_async(
                process, (sku, table1, dict1, lock, cookie_pool,), callback=update)
        workers.close()
        workers.join()
        table1.insert(0, ['SKU', 'option', 'full_sku', 'title', 'stock','price','rate','reviews','sale_tag'])

        csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Wayfair_PriceOutput_' + \
            date + '_' + country + '.csv'
        with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
            writer = csv.writer(f, dialect='excel')
            writer.writerows(table1)
        mapping_sku(
            csv_path1,
            r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_Mapping_{}.csv'.format(country),'full_sku','partner_number')
    e = time()
    bot_push_text('{}\n总用时：{}s'.format(os.path.basename(__file__),strftime("%H:%M:%S", gmtime(e - s))))
