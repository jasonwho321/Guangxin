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
    print(final_cookies)
    chrome.quit()
    final_cookies = 'dt=2022-11-02T10:21:05.421Z; ostk_campaign=""; se_list=se_list^0|82|181|55|9|; ostk_aggr_year2=ocode^341f1900-4f73-11ed-98c5-793418d46ae7; ostk_aggr_year=country^HK|currency^HKD|language^en|mxcuserseed^8772844656344280883; _fbp=fb.1.1666159175825.1794942266; _gcl_au=1.1.201384997.1666159176; _abck=6D36EF11643C013E6302130870777BB5~0~YAAQtetGaLwGgTWEAQAAVMPJNwjBnWA7mLZtaHyDrnboBNMYsGPAyECxuTNNCRQlcyXTIemjYNJCxOnWS3vCjsyZjrQEqv7d0l3tlJ34Vubl/EjzeIlQ1Je1dX8CmZWBcEqZjqeJgaXMs3E9GxNOanJNte7kQvRHC42y5P4qBp8UFq5jA9vLmsenOyAWdtZAdtmGO4Rr0BK7mgx23UpdC3Z0iSzro8PLmOmabSWQ29qSYfyDGL5iLw5c1n5demOnBJeCvP2uOedbvoQeZ2FjXM5jbNAV2wwHeCeGzNWPoHKvqBrPMVtCfLECDBrp645tXQdsRWOe0yJlBU0uxBga0QxFrLJEUk6Hli2Wzvysf2iccytROJB0/v7DOcRWRKQQv2ydn7hn5kQh0l9OYPVIY5dBoHvx41R/xvlkRg==~-1~-1~-1; bm_sz=B2BB45956D7CBF9EF96FF8A7D99FB54A~YAAQtetGaL0GgTWEAQAAVMPJNxFSjAG1svoPoPMMJVOpO6Lq1cARFy1TIW2NPbqI8goZST7pb6qQMZbSlH4xVcILkDnoceAtEkgw2H63bc2HQOUjAF+oTyIXyMUigLpx0esdCpESDMZKt8LuLtYLiaHBm9CHJRCklIFE2eCbisuak1Vz/Q77cpu0U2fc11YZIN0WxIAqep6bJuS4uBLrCwCGy1gmR0rOPFb8JSCVA4ppM1oilU1R2ih6cfRAnFFXUNDqRhge74ewnTH1p2OqDQ0tykfwI4PHldFBInU63/jDSX78Abs=~4473414~4407604; AKA_A2=A; mxclastvisit=20221102; mxcsurftype=8; bm_mi=3A28BD358EC3D6A030BC1826589D5070~YAAQtetGaCUHgTWEAQAArsrJNxFnCrtNwK24Q7huJNH9XVQwfHZ8fuCaEw2ynOO2HIO7vo/kdLXRJHTFrK5P6VjURKicgpwdFXvn4SzoB3u/dD2+ZxcBQUI70zc5l5g04GVI3DsD+wpB1KtZCmm/lHQTCpmIgS0/LslYrRQhl0yM0XGQYgZgn26uhg3hR2qVv+pENHve2gV8CAbFPzYtssuqg4PnlJxz87FsDed6jJgfWq9DJc3TaYNnZ5AHH76xoINI/HaVyM6KVi3jcMu0SmRvVX7bFbDDW4vYpNvp0ojBHHk/aXxNknhX9gTksnVf3LtM/wlX5ohM3ijselhFDx1ih0Gwcw9PSAVAM3c5ymA1DidXVK+6mcpZT4A03f7Qebn5vfgUpBXp+K8Bs0IJGZ+SyCxf92om4ES2oEMrRsgknZU8Ng2UcWq8/Ir0f+mn6B8c7g2teCLb~1; fngprnt="N4IgSgpgzg9gNgVwC4EsYDsQC4QEYBsAHAAwA0uxArMSKSAEIBOMA7lBIwAQAmKAthHRQ0Q7HnwBmAEykAnMQDstEABV+EAF4YInGADM97JJz3M+nAKoqAwpxTpOfe8mjYAtABYSda/BhduCAAHJAALbCkPOgApAEMAN1jOQViAIzgIbmw9WLh2OgAZGABjXIgxDVC3awA5ZQBRdGKYXnQAczErADE3QmUi0rhOAGUkf1i2nUa0jKysJEYECDomVnYuAAVENvsoMQBtAB0QDYARLs4ANRQIFg5j0mPrULMdM4vr2/vaJ5eYJwQ5neVxud0YD2OAFkUMVmLA9MZ6txJpxgZ8wRCQAB1CCpADSKGMqQQKDgSDc9lR52OAF0QABfIA="; _gid=GA1.2.1747554033.1667383328; cto_bundle=UloEvF85Ujk4cXg4SWE0a3I2RkdZNzYyZDglMkZGc3BUNFkyYXMzYjdPUzZzdW01VjE5dlNrbjZRakV6YldSbWpYOTBiRWJKdEg1eFhyS3NtS2FuJTJGY1hMcmNGT0ZBNEI2ODVua1RnaGZ4dk5WZUlrOERuQjIxYUpMOUtpZmc4U3prN0RkWFVLNHM0S3ZyaFJha2dzN0tZeE02cnlBJTNEJTNE; ab.storage.deviceId.5c2ca4f1-0219-4717-859b-ca7dceb0be43={"g":"7eacb020-ee2b-5b2d-2d5b-877b0d8ec19d","c":1666159176368,"l":1667383328846}; mpid=2645313826893697658; _clck=1lrflhv|1|f68|0; _pin_unauth=dWlkPU9EazRNelpqTm1NdE5tVXlOUzAwTm1RM0xXSTVaRFV0TTJKaU1qWmhOR0V4T0RrMA; ak_bmsc=BCB28C996B6B1D7563CE98944F70165C~000000000000000000000000000000~YAAQtetGaJAWgTWEAQAArITKNxEINzOrjDudbOQEXkAblWoPWTMTJ3Eh7M5qjgEh+4cfXyVDUCBIfdYhNucBjV4UIPhrHkmtLMr0orTmO/NtJTNF3BMraL00i1t62dane/4wUzKxi0YsbcwXgdxNh2np63V91op9R7rTSyofmPDmFDmwuik70wf8d7RdoXwP8bNCdsoH6cc6dFveuUyT50vfFQH3LgQ8KkpFB4VnT6BOxcIw2sp4dEBLIlf9kJQdQR3LBTOCsion3XatkKELiSkzr7OFi854BSFzpTB9qZeLxsE6AwypqeMuqUVQaBgEhFcHRKA/AaM9FmwfDr9g3ftISax22IpK23yDeII3eIjN1AeDSGUlLmoBKpM87WN9ApEZ1bKnVFE/y1nmWkY9Tdnqy4AZ+/GG4+6uGjJh3XIy9iy7qrOsL1fn+dXj6ou3THSoxq1ZL5SCaGtYvfDSijLryrzu7iqm9vxn5fe8vv8CHaoNjnU4kj5M/H1OAefKD5GJ9GuVv7hAvDsxZ46j9mjFkLSzdaRtg5tcQS5XFRavHc2qR1qSpSv0m1itb44IRAF7XZogmz2ZdWZjbgH5shONYKv7PBhLkTIH2q5muNx0; ostk_aggr_session=octs^1667384461001|sessstrt^1667383281888|billingcountry^US|gcr^false|cart.item-count^0|dlp^k|postal^00000; mxcproclicks=28226797|28081512|34180829|; pageTriggers=gdpr^2|countrySelect^1|gdprExpiration^1675160463766|pushNotifications^1|triggerFiredTime^1667384463773; _gat_gtag_UA_22002224_1=1; QSI_HistorySession=https://www.overstock.com/Home-Garden/FurnitureR-Home-Office-Velvet-Patchwork-Lounge-Chair-Swivel-Task-Chair/28081512/product.html~1667383720391|https://www.overstock.com/Home-Garden/FurnitureR-Dining-Table-Metal-Frame-Glass-Table-Top-White/28226797/product.html~1667384469663; _uetsid=692d52005a9511ed8cb49ff91293226d; _uetvid=359666504f7311ed8d25939ece93a763; ostk_profile=firstPageViewed^https://www.overstock.com/Home-Garden/FurnitureR-Home-Office-Velvet-Patchwork-Lounge-Chair-Swivel-Task-Chair/28081512/product.html|lastPageViewed_prev^https://www.overstock.com/Home-Garden/Homylin-Set-of-2-40-Upholstered-Bar-Chairs-with-Footrest/34180829/product.html|lastViewedSubCategory_prev^null|referrer^null|lastViewedSubCategory_cur^null|lastPageViewed_cur^https://www.overstock.com/Home-Garden/FurnitureR-Dining-Table-Metal-Frame-Glass-Table-Top-White/28226797/product.html|ocode^null|newVsExisting^null|pdpIsFirstTouch^false; ab.storage.sessionId.5c2ca4f1-0219-4717-859b-ca7dceb0be43={"g":"ef380818-11dc-1072-723c-af1955fa446c","e":1667386280912,"c":1667383328845,"l":1667384480912}; _clsk=1ue2tnp|1667384481881|4|0|n.clarity.ms/collect; utag_main=v_id:0183eed367a7001d154e5758caf805081007407900ac2$_sn:2$_se:9$_ss:0$_st:1667386286825$dc_visit:2$ses_id:1667383327684;exp-session$_pn:3;exp-session$dc_event:6;exp-session$dc_region:ap-east-1;exp-session; _ga_8MPQ3CZZFH=GS1.1.1667383720.1.1.1667384486.36.0.0; _ga=GA1.1.0183eed367a7001d154e5758caf805081007407900ac2; bm_sv=B2FE16B61196EDF98A4E9D304B41F3DF~YAAQtetGaLv8gjWEAQAAfIvcNxEXul+F1XoaJjYKN1YHuAgyyMzN0wmUTKIJGPy66xjaQG9tRLrLydh99Zto0HzY9NB0mcvAgPzvcJiXXAIySYql3ieMN99Y1NHY+i0KLchKjnnFk2IYOj8tVdk5J7XCgPMmIptzHf+lrIiSBY19EGlEi4NkEUpzNWIAeTbXw007OeDOooUjzgjmmogCQfkcXWO+jUK8UpYKzqcBeKo3FlWfIOEcoHspp8Dfe1WR2QHndw==~1'
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
        print(url)
        sp = requests.session().get(url, proxies={"http": "http://{}".format(proxy)}, headers=headers,
                                    allow_redirects=False)
    content = sp.content
    soup = BeautifulSoup(content, "html.parser")
    print(soup)
    result = 0
    while result == 0:
        sleep(3)
        try:
            e = soup.find_all('div', class_="search_search_77")
            e=e[0]
            result = 1
        except BaseException as B:
            print(B)
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