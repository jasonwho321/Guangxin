import csv
from datetime import datetime
import json
from selenium import webdriver
from time import time, strftime, gmtime
import pandas as pd
from multiprocessing import Process, Manager


def get_info(link, table1, soup):
    try:
        options = soup['props']['pageProps']['product']['options']
        for option in options:
            price = option['price']
            subsku = option['subSku']
            decription = option['decription']
            qtyonhand = option['qtyOnHand']
            output = [link, subsku, '-'.join([link[-8:], subsku]), decription, price, qtyonhand]
            print(output)
            table1.append(output)
    except BaseException as e:
        print(e)
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


def mapping_sku(csv_priceout, csv_map):
    df_price = pd.read_csv(csv_priceout)
    df_map = pd.read_csv(csv_map)
    df_price['OSSKU'] = df_price['OSSKU'].str.strip()
    df_price['PartNumber'] = df_price['OSSKU'].map(df_map, na_action=None)
    df_price.to_csv(csv_priceout)


def process(num1, num2, table1):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome = webdriver.Chrome(r"D:\chromedriver.exe", options=chrome_options)
    csv_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\SKU_list.csv'
    data = read_src(csv_path)
    for link in data[num1:num2]:
        chrome.implicitly_wait(20)
        print("总体进度：{}/{}".format(data.index(link), len(data)))
        link = link[0]
        try:
            chrome.get(link)
            ele_list = chrome.find_element_by_id("__NEXT_DATA__")
            soup = json.loads(ele_list.get_attribute('innerHTML'))
            table1 = get_info(link, table1, soup)
        except:
            table1.append([link, '-', '-', '-', '-'])


def main():
    csv_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\SKU_list.csv'
    data = read_src(csv_path)
    lenth = len(data)
    len1 = round(lenth / 4)
    len2 = round(lenth / 2)
    len3 = round(lenth * 3 / 4)

    date = datetime.today().strftime("%Y%m%d")
    process_list = []
    manager = Manager()
    table1 = manager.list()  # 也可以使用列表dict
    p1 = Process(target=process, args=(None, len1, table1))
    p1.start()
    p2 = Process(target=process, args=(len1, len2, table1))
    p2.start()
    p3 = Process(target=process, args=(len2, len3, table1))
    p3.start()
    p4 = Process(target=process, args=(len3, None, table1))
    p4.start()

    process_list.append(p1)
    process_list.append(p2)
    process_list.append(p3)
    process_list.append(p4)

    for t in process_list:
        t.join()
    csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\Overstock_PriceOutput_' + \
                date + '.csv'
    table1.insert(0, ['Link', 'subSKU', 'OSSKU', 'Color', 'price', 'OnHand'])
    with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerows(table1)
    mapping_sku(csv_path1, r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\SKU_Mapping.csv')


if __name__ == '__main__':
    s = time()
    main()
    e = time()
    print('总用时：{}s'.format(strftime("%H:%M:%S", gmtime(e - s))))
