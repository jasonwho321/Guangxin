import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import json
import random
from selenium import webdriver
from time import sleep, time,strftime,gmtime
from multiprocessing import Process,Manager
def get_info( table1,soup):
    try:
        options = soup['props']['pageProps']['product']['options']
        for option in options:
            price = option['price']
            subSku = option['subSku']
            decription = option['decription']
            qtyOnHand = option['qtyOnHand']
            output = [link,subSku,decription,price,qtyOnHand]
            print(output)
            table1.append(output)
    except Exception as e:
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
if __name__ == '__main__':
    s = time()
    date = datetime.today().strftime("%Y%m%d")
    table1 = []
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome = webdriver.Chrome(r"D:\chromedriver.exe", options=chrome_options)
    csv_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\SKU_list.csv'
    data = read_src(csv_path)
    for link in data:
        chrome.implicitly_wait(20)
        print("总体进度：{}/{}".format(data.index(link), len(data)))
        link = link[0]
        try:
            chrome.get(link)
            ele_list = chrome.find_element_by_id("__NEXT_DATA__")
            soup = json.loads(ele_list.get_attribute('innerHTML'))
            table1 = get_info(table1, soup)
        except:
            table1.append([link,'-','-','-','-'])
    csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\OS爬虫\Overstock_PriceOutput_' + \
        date + '.csv'
    with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerows(table1)
    e = time()
    print('总用时：{}s'.format(strftime("%H:%M:%S", gmtime(e - s))))