import csv
from datetime import datetime
import json
from selenium import webdriver
from time import time, strftime, gmtime, sleep
import requests
import random
from bs4 import BeautifulSoup


link_list_US = ['http://www.walmart.com/ip/HouseInBox-Computer-Desk-Study-Writing-Table-Workstation-Organizer-with-Shelves-for-Home-Office-Use/262695439',
                'http://www.walmart.com/ip/HouseInBox-Metal-Platform-Bed-Frame-Gold-Daybed-Metal-Platform-Bed/381052787',
                'http://www.walmart.com/ip/HouseInBox-Industrial-29-Inch-Bar-Stools-Set-of-2-Home-Bar-Furniture-Stools-Farbic-Seat-and-Back-with-Metal-Legs-for-Kitchen-Dining-Room-Yellow/520464319',
                'http://www.walmart.com/ip/HouseInBox-29-Comfortable-Counter-Height-Bar-Stools-Set-of-2-360-Degree-Swivel-Seat-Height-Back-Bar-Stool/1289632667',
                'http://www.walmart.com/ip/HouseInBox-Mid-Century-25-Counter-Height-Metal-Bar-Stools-Set-of-2/771125348',
                'http://www.walmart.com/ip/HouseInBox-Home-Office-Computer-Desk-Chair-Velvet-Upholstered-Armchair-Open-Back-Swivel-Work-Arm-Chair-Blue/676448348',
                'http://www.walmart.com/ip/HouseInBox-Velvet-Accent-Chair-for-Living-Room-Light-Green-Wing-Back-Armchair-Tufted-Back-Upholstery-Living-Room-Chairs/509331187',
                'http://www.walmart.com/ip/HouseInBox-Outdoor-Dining-Table-Set-for-4-Metal-Solid-Wood-Kitchen-Dining-Room-Sets-Restaurant-Table-and-Chairs-Set-Outdoor-Need-Under-Cover/1062943959']
link_list_CA = ['https://www.walmart.ca/en/ip/homycasa-brown-l-shape-desk-open-storage-mdf-wood-spacious-extra-storage-shelves-table-brown/PRD65SS591ODNJ7',
                'https://www.walmart.ca/en/ip/homycasa-computer-desk-with-rolling-convertible-shelf-walnut-walnut/6000203334780',
                'https://www.walmart.ca/en/ip/homycasa-computer-desk-teen-writing-desk-with-drawer-and-keyboard-tray-oakwhite-oakwhite/6000202560753',
                'https://www.walmart.ca/en/ip/homycasa-gilda-325-in-white-moderncontemporary-writing-desk-white/6000205673320',
                'https://www.walmart.ca/en/ip/office-chair-gray-lumbar-support-mesh-computer-desk-task-chair-gray/6000202491534',
                'https://www.walmart.ca/en/ip/office-chair-purple-lumbar-support-mesh-computer-desk-task-chair-purple/6000202491771',
                'https://www.walmart.ca/en/ip/office-chair-mint-green-lumbar-support-mesh-computer-desk-task-chair-green/6000202491904',
                'https://www.walmart.ca/en/ip/homycasa-fiyan-acrylic-30-in-black-fixed-height-bar-stool-set-of-2-black/6000205673948',
                'https://www.walmart.ca/en/ip/falette-green-tufted-velvet-arm-chair-green/6000202563262']
csv_path_CA = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Walmart爬虫\SKU_list_CA.csv'
csv_path_US = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Walmart爬虫\SKU_list_US.csv'


def get_cookies(country):
    link_list = link_list_US if country == "US" else link_list_CA
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome = webdriver.Chrome(r"D:\chromedriver.exe", options=chrome_options)
    i = 1
    while i < 20:
        sleep(60)
        new_tab(chrome, link=random.choice(link_list), i=i)
        if chrome.current_url.startswith(
                'https://www.walmart.com/blocked?') or chrome.current_url.startswith('https://www.walmart.ca/blocked?'):
            new_tab(chrome, link=random.choice(link_list), i=i)
            i += 1
        else:
            i = 20
    # 就当成js语句吧
    sleep(3)
    cookies = chrome.get_cookies()
    final_cookies = ''
    for cookie in cookies:
        item = cookie['name'] + '=' + cookie['value'] + '; '
        final_cookies = final_cookies + item
    final_cookies = final_cookies[:-2]
    chrome.quit()
    return final_cookies


def new_tab(chrome, link, i):
    newTab = 'window.open("{}","_blank");'.format(link)
    chrome.execute_script(newTab)
    windows = chrome.window_handles
    chrome.switch_to.window(windows[i])
    chrome.implicitly_wait(20)


def get_info(link, table1, soup):
    try:
        product = soup['props']['pageProps']['initialData']['data']['product']
        if product['variantProductIdMap']:
            variantProductIdMap = product['variantProductIdMap']
            for i in variantProductIdMap:
                color = i
                id = variantProductIdMap[i]
                option = product['variantsMap'][id]
                price = option['priceInfo']['currentPrice']['price']
                avlib = option['availabilityStatus']
                output = [link, color, price, avlib]
                # print(output)
                table1.append(output)

        else:
            price = product['priceInfo']['currentPrice']['price']
            avlib = product['availabilityStatus']
            output = [link, '-', price, avlib]
            # print(output)
            table1.append(output)
    except BaseException as e:
        # print(e)
        pass
    return table1


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


# def mapping_sku(csv_priceout, csv_map):
#     df_price = pd.read_csv(csv_priceout)
#     df_map = pd.read_csv(csv_map)
#     df_map = df_map.to_dict('list')
#     df_map = dict(zip(df_map['OSSKU'], df_map['Partner SKU']))
#     df_price['OSSKU'] = df_price['OSSKU'].str.strip()
#     df_price['PartNumber'] = df_price['OSSKU'].map(df_map, na_action=None)
#     df_price.to_csv(csv_priceout)
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


def process(table1, country):
    final_cookies = get_cookies(country=country)
    csv_path = csv_path_US if country == "US" else csv_path_CA
    data = read_src(csv_path)
    n = 0
    headers = {
        'cookie': final_cookies,
        'user-agent': get_ua(),
        'upgrade-insecure-requests': '1'
    }
    while n < len(data):
        print("总体进度：{}/{}".format(str(n), str(len(data))))
        link = data[n][0]
        sp = requests.session().get(link, headers=headers)
        if sp.url.startswith('https://www.walmart.com/blocked?') or sp.url.startswith(
                'https://www.walmart.ca/blocked?'):
            headers = {
                'cookie': final_cookies,
                'user-agent': get_ua(),
                'upgrade-insecure-requests': '1'
            }
            continue
        try:
            content = sp.content
            soup = BeautifulSoup(content, "html.parser")
            text = soup.find('script', {'id': '__NEXT_DATA__'}).get_text()
            soup = json.loads(text)
            table1 = get_info(link, table1, soup)
        except BaseException:
            table1.append([link, '-', '-', '-'])
        n += 1
    return table1


def main(country):
    date = datetime.today().strftime("%Y%m%d")
    table1 = []
    process(table1, country)
    csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Walmart爬虫\Walmart_PriceOutput_' + \
        date + '_' + country + '.csv'
    table1.insert(0, ['Link', 'color', 'price', 'stock'])
    with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerows(table1)
    # mapping_sku(
    #     csv_path1,
    #     r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Walmart爬虫\SKU_Mapping.csv')


if __name__ == '__main__':
    s = time()
    main('US')
    e = time()
    print('总用时：{}s'.format(strftime("%H:%M:%S", gmtime(e - s))))
    s = time()
    main('CA')
    e = time()
    print('总用时：{}s'.format(strftime("%H:%M:%S", gmtime(e - s))))
    # mapping_sku(r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\Overstock_PriceOutput_20221110.csv', r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\SKU_Mapping.csv')
    # mapping_sku(r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\Overstock_PriceOutput_20221110.csv', r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\SKU_Mapping.csv')

