# -- coding: utf-8 --**
import csv
import os
from datetime import datetime
import json

import pandas as pd
from selenium import webdriver
from time import time, strftime, gmtime, sleep
import requests
import random
from bs4 import BeautifulSoup
from tqdm import tqdm
from scrapy_file.Wayfair_Scrapy import read_src, get_ua
from guangxin_base import bot_push_text
import requests
import socks
import socket
# 设置默认的 SOCKS5 代理
socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", 7890)
socket.socket = socks.socksocket

proxyip = "http://storm-maplin_area-US_city-LosAngeles_life-1:Homycasa2012@proxy.stormip.cn:1000"

proxies = {
    'http': proxyip,
    'https': proxyip,
}
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
csv_path = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/Walmart爬虫/!运营维护_Walmart_SKU爬虫清单.csv'

def get_cookies(country):
    link_list = link_list_US if country == "US" else link_list_CA
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option(
        'excludeSwitches', ['enable-automation'])
    chrome_options.add_argument("--disable-blink-features")
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome = webdriver.Chrome(r"D:\chromedriver.exe", options=chrome_options)
    i = 1
    while i < 20:
        new_tab(chrome, link=random.choice(link_list), i=i)
        if chrome.current_url.startswith(
                'https://www.walmart.com/blocked?') or chrome.current_url.startswith('https://www.walmart.ca/blocked?'):
            i += 1
            sleep(60)
            pass
        else:
            break
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


# def mapping_sku(csv_priceout, csv_map):
#     df_price = pd.read_csv(csv_priceout)
#     df_map = pd.read_csv(csv_map)
#     df_map = df_map.to_dict('list')
#     df_map = dict(zip(df_map['OSSKU'], df_map['Partner SKU']))
#     df_price['OSSKU'] = df_price['OSSKU'].str.strip()
#     df_price['PartNumber'] = df_price['OSSKU'].map(df_map, na_action=None)
#     df_price.to_csv(csv_priceout)


def process(table1, country):
    # final_cookies = get_cookies(country=country)
    final_cookies_US = 'ACID=2e100910-cace-4be5-8db1-14080e0d2da8; hasACID=true; assortmentStoreId=3081; hasLocData=1; vtc=Tf3lMpGE3WqgTY-974ZLIM; _pxhd=e7aec1b057e2f528f109fa9dcd080e576cd3031b8b453a2e5457e7d025d28ce3:81b2c800-64b4-11ed-bdba-634167735176; adblocked=false; _pxvid=81b2c800-64b4-11ed-bdba-634167735176; TBV=7; AID=wmlspartner=0:reflectorid=0000000000000000000000:lastupd=1668496203488; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJzdG9yZVNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQiLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTY2ODQ5NjE5NTE5MX0sInNoaXBwaW5nQWRkcmVzcyI6eyJpZCI6bnVsbCwidGltZXN0YW1wIjoxNjY4NDk2MTk1MTkxLCJjcmVhdGVUaW1lc3RhbXAiOm51bGwsInR5cGUiOiJwYXJ0aWFsLWxvY2F0aW9uIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJwb3N0YWxDb2RlIjoiOTU4MjkiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJkZWxpdmVyeVN0b3JlTGlzdCI6W3sibm9kZUlkIjoiMzA4MSIsInR5cGUiOiJERUxJVkVSWSJ9XX0sInBvc3RhbENvZGUiOnsidGltZXN0YW1wIjoxNjY4NDk2MTk1MTkxLCJiYXNlIjoiOTU4MjkifSwibXAiOltdLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6MmUxMDA5MTAtY2FjZS00YmU1LThkYjEtMTQwODBlMGQyZGE4In0=; TB_SFOU-100=; TB_Latency_Tracker_100=1; TB_Navigation_Preload_01=1; pxcts=8b6f5d25-665e-11ed-806d-446658496264; auth=MTAyOTYyMDE4I/ef0poEIHUJM07GEHZB4jf/cqqLDYe9LPq6IFzk1N/XO5+qHmzOnxSd6Jm+8Ad3QF/SGFX5qI8EGnd2cY3qrTv6pukT8JWla1CCY+Z3vhCYybEajj235v6w6v39wTDg767wuZloTfhm7Wk2KcjygjFwIZIekXC4wlSRgDWHtlx2a/zW0L/KPXTet227kf4xNfl0FZJL+RDUkrRPTKcqL1bqyVfSOR4SVKnshTQTbqAUMk70P8glgOEpLOprhDfMM/FHGZ2dCNmxWrdkwqEKrnl/V3VBWwHli1+5Azg3dheAQZsLlpiMJES9cEt5Lv7gzfpRoDFNXw87ntrti19CU2CbUBplgcR/Zb5ok1TYnyOVgpOWSPOn9jYl7hzv88D2nPtOadWNz0/1FF56DlGrDkjyrOXbKKhH072NS/W0j/U=; bstc=T4xFKPcfdxSVrwtmtSGfoI; mobileweb=0; xptc=assortmentStoreId+3081; xpth=x-o-mverified+false; xpa=; xpm=3+1668736472+Tf3lMpGE3WqgTY-974ZLIM~+0; _astc=81c29cb11232a6dfbab6b10f7962003d; _pxff_cfp=1; ak_bmsc=7335AA78C26AEE6A7E05374C6BBC1728~000000000000000000000000000000~YAAQN6omF+WzroKEAQAAmepxiBE2yt9nsier8zCljSpaVKogVrkKB+Q219n0Gh6hHCphkOd+mCrQOwTgG36NDzXcDIzUVuk7JEG2PcTU0ibxDuhroEmaC6mLPJf3FQ0jB0hQsQCiAjmvJaFDVU1Dp7GgTFUVhdONxr5TuW1/jmHyueZVp4dc+Oa+mIsZTCtbb8mBxU2vdceKHM8yIEFASc/o40KTYO0bEYkTIuEijSZ9R8LVJa7Onz9Si5q7iZ4Ue3LIKTVJoArHba7JIUFTq9Fmf/OVEdt4VlplRB07D6Q6WWxHXJ9YDQc2M20fl1iQFSmvPRGsqtkLbU48QNMQswJXuOAC0TpE2YY8Bm5U0swY3nOtFDA//FoULxHZpTC7R4ko2QhRS8z7UcFqCfS3LjPO8PPtRc5sHpRMz6SPadXxwk0Ae8AdUn/Wnl+ZmqqWGLCCP4nFg/BF/TV5kqoWznw5GJEG49DvDY+0VxbCeyqb6dcjWVcDb7Kc; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdlcmJlciBSb2FkIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIzMDgxIiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9JTlNUT1JFIiwiUElDS1VQX0NVUkJTSURFIl19XSwic2hpcHBpbmdBZGRyZXNzIjp7ImxhdGl0dWRlIjozOC40NzQ0LCJsb25naXR1ZGUiOi0xMjEuMzQzNywicG9zdGFsQ29kZSI6Ijk1ODI5IiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeUNvZGUiOiJVU0EiLCJnaWZ0QWRkcmVzcyI6ZmFsc2V9LCJhc3NvcnRtZW50Ijp7Im5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJhY2Nlc3NQb2ludHMiOm51bGwsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbXSwiaW50ZW50IjoiUElDS1VQIiwic2NoZWR1bGVFbmFibGVkIjpmYWxzZX0sImRlbGl2ZXJ5Ijp7ImJ1SWQiOiIwIiwibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsIm5vZGVUeXBlIjoiU1RPUkUiLCJhZGRyZXNzIjp7InBvc3RhbENvZGUiOiI5NTgyOSIsImFkZHJlc3NMaW5lMSI6Ijg5MTUgR2VyYmVyIFJvYWQiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJjb3VudHJ5IjoiVVMiLCJwb3N0YWxDb2RlOSI6Ijk1ODI5LTAwMDAifSwiZ2VvUG9pbnQiOnsibGF0aXR1ZGUiOjM4LjQ4MjY3NywibG9uZ2l0dWRlIjotMTIxLjM2OTAyNn0sImlzR2xhc3NFbmFibGVkIjp0cnVlLCJzY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOnRydWUsImFjY2Vzc1BvaW50cyI6W3siYWNjZXNzVHlwZSI6IkRFTElWRVJZX0FERFJFU1MifV0sImh1Yk5vZGVJZCI6IjMwODEiLCJpc0V4cHJlc3NEZWxpdmVyeU9ubHkiOmZhbHNlLCJzdXBwb3J0ZWRBY2Nlc3NUeXBlcyI6WyJERUxJVkVSWV9BRERSRVNTIl19LCJpbnN0b3JlIjpmYWxzZSwicmVmcmVzaEF0IjoxNjY4NzU4MDc3OTg3LCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6MmUxMDA5MTAtY2FjZS00YmU1LThkYjEtMTQwODBlMGQyZGE4In0=; xptwj=rq:90ff07714a2a6c99532c:Wb/9QjAfEz9z8TsBeP/HqFZgCH/8aO54SLeX45yArn6KM5cPpTLNpdAYhGLpEXi3xxGTwkREPsQ6BNZardOg7Z/0QqXzisVxuwW71Uzp9LTYuN5V7thm1cR+zkw1YOD72appXoVs+xllEgc=; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1668736482000@firstcreate:1668496195151"; akavpau_p2=1668737082~id=ca5a20238b296445c267b19a5a9d1136; xptwg=1191993963:113660ED511ACA0:2C4D112:8407952A:920AF50A:8B1F47B:; TS012768cf=016de4b761f155002003f9687f8ebfb2f08d471edc9cd81c85c48cf720652f47be26c5bc29ee60033d4a589514205219b8a1b0dab8; TS01a90220=016de4b761f155002003f9687f8ebfb2f08d471edc9cd81c85c48cf720652f47be26c5bc29ee60033d4a589514205219b8a1b0dab8; TS2a5e0c5c027=08ad589ecdab2000a27f97095c2bebe555593ff77583ba4551fabbecd9e8d8e8938d90b5850534f8088e5917901130005c536878f2c2865c8b6b8a9e4f0b87000936fb5f684cf51234da2dd0de4249a14d32ff8e29254c1ac87c78c18321cb5b; bm_sv=211E9317B96D834493A1F3D055C324D0~YAAQN6omF5K1roKEAQAAwftxiBENi46F72ZtjcGvELmi5lnJxwsNndXcWko4F/bczSjxwmvi38tw/sySJJ5wbmCVCrLKZyKT8e872n6nSkG0MBTztYEcyni0tP6tYKCirGaU2/8aG7RkJ0xnSAybtlkuhoyfpHqOrsG6UZkaPmfP/MX3Uknv0YTZX8IqNox/RO4SNoQjh+LKQpxAF16J1L4wjWv27W8HBD0+DP0kVsStu25x6NC0aaRyf41PVcg1QQ==~1; _px3=23d33b1007339c33fe145bde7bea423cdd42d154e667437690ad83a698838ca0:fFax+jyvTyP70bFojBLRhd5/+i+5Ll9usKPCGisgENLKD0+dcE9/uu6wY9QOsnPklX3EAgIyHzGrgj+h7Lpk8g==:1000:gawNweeBNibNR+TI1dhISXdfYif5ZDkVIulUD6PScelrtiIjzUzldnOIvss+7y2W6F1l6uLPC1MaRLvWlkqRhLAggfwG4PBpkfVY/gkESh3gDOuwrLFr0W5CFz1zm4Ruu2CBYATkhgEFsKVLUlopy6Vnlbo9Tp9YKn2euXQirDg7HA7g7mKzuUY2Ah7KUF3Up8abrPyo+2ex27eowdX9wg=='
    final_cookies_CA = 'localStoreInfo=eyJwb3N0YWxDb2RlIjoiTDVWMk42IiwibG9jYWxTdG9yZUlkIjoiMTA2MSIsInNlbGVjdGVkU3RvcmVJZCI6IjEwNjEiLCJzZWxlY3RlZFN0b3JlTmFtZSI6IkhlYXJ0bGFuZCBTdXBlcmNlbnRyZSIsImZ1bGZpbGxtZW50U3RvcmVJZCI6IjEwNjEiLCJmdWxmaWxsbWVudFR5cGUiOiJJTlNUT1JFX1BJQ0tVUCIsImFzc2lnbmVkRmFsbGJhY2siOnRydWV9; deliveryCatchment=1061; walmart.nearestPostalCode=L5V2N6; walmart.shippingPostalCode=L5V2N6; defaultNearestStoreId=1061; vtc=UXumcqH8VGJvd_8jF3BM8M; walmart.nearestLatLng="43.60822,-79.69387"; userSegment=50-percent; walmart.id=a4630eb8-84c5-45a9-9dff-19c89f92d7f2; _pxvid=b71ae2e1-6590-11ed-a4af-66647043496f; _cs_c=1; _gcl_au=1.1.7570676.1668590776; _4c_={"_4c_mc_":"2ad89d60-e6d4-4925-9121-d2c20aea8d1e"}; _ga=GA1.2.532819604.1668590775; __gads=ID=5ad3847dd73d0503:T=1668590774:S=ALNI_Ma-04Bn25sImXgDcacY4GO7VPYJqA; s_ecid=MCMID|90213565073727342430939047591393783286; _fbp=fb.1.1668590776620.1726878515; headerType=whiteGM; ENV=ak-eus-t1-prod; bstc=apDtAzn1CNjv-MUJdjq3CE; xpa=1-G2r|D1wOk|HfmFY|LQ224|Otf8Q|QlFXu|_APy0|xpFrO; exp-ck=1-G2r1D1wOk1HfmFY1Otf8Q1QlFXu1_APy01xpFrO1; ak_bmsc=843BAB8A3215E9F0066D81B049540CCF~000000000000000000000000000000~YAAQMGdNaHrZ7n6EAQAAs2WBiBFtBHy2FNyAzRoKV1jibN6u1NvPysbrT0uax5TELr4o7prymIUD26AW7Rg7EArOGQzlfcSbUhoxgkP67h5J+pihiuDRMORF1rEqCGw9FAVOfDCAhK2AS+Ixgj5PkAsGGKiQBKYdXerznK99hjksACroVXaQrAjmJg7mgwmUCe7wG4mh+niSSxWdtEsRDCd/SX8b9YIZJqkeh2e2KoYiJ9+5uhu/r1OLdnHkxG9dt54uJfRiOTgAM7i+IWBV6VtmGDT6VWPsJmK7KHTBkxVt6nMD5o8hnJry6/RZHfqrG/2klVposAht+2KXXKk07Lw7EQxmbO3jiz1rnkI3PO8Owy6w0XDaIEGRIuqztc0WGqmk5z/LiPXWJA==; enableHTTPS=1; _uetsid=53ff97b066e611edb609bfd9bc515cba; _uetvid=b77efc40659011eda161fd77802f51ed; pxcts=53b01a1c-66e6-11ed-8a2c-496d48567064; kndctr_C4C6370453309C960A490D44_AdobeOrg_identity=CiY5MDIxMzU2NTA3MzcyNzM0MjQzMDkzOTA0NzU5MTM5Mzc4MzI4NlIPCMvZiv7HMBgBKgRTR1Az8AH75oXEyDA=; kndctr_C4C6370453309C960A490D44_AdobeOrg_cluster=va6; AMCVS_C4C6370453309C960A490D44@AdobeOrg=1; _px3=c17e49103929f44da795ba2eec99aff72ac270577711f175649862f9b1e2f5df:cVf+GIvc1MMkwA3rlYmWxGEiJNSv0tdg9pqpcr2xkLSRmmIM4DfojvxS+A+ScJzeghulW75Ivd63+aA29dp1Fg==:1000:QZ+oLuVTyG5c3nmCeWH9TW0wzbskqQtslwiErowny6IWOWiFuQEXuzGZjZNadpOM15+A7wjwCM/Nw38VWH4kZiCbmpyyH0kr3+cgfaTZYS3LWzpNOIiQ1psLeI6SsktHznHBvGbJyrBpy1cKqgjS8HfQIxr+RfWoHAoUB8JfPLyzR65pvQLa//bvu+a2MKxLPbteFIO/6aOd21RzdBrHkw==; cto_bundle=Xlcot19DOHQ1JTJCamh6M3cxMDZaSlhlVjI0aUNwQUhUZzhuJTJGQUlZVThZUzNRejVEdnV0TmhLM1d3N2JwRFpvd0hKY0pCNFJ4NkxYT21WbUV0dWdEdnViMnJreW95MVJReXQ5cHRoT0FEbnFZcW5nNFN0eU1aUFFHM3l4MnhseGRuTGZlNHlhc0g2bjFVVUNhUHkxNSUyRkxmRDkyM1ElM0QlM0Q; s_gnr=1668737498104-Repeat; _cs_mk_aa=0.8788400890423107_1668737498104; s_visit=1; gpv_Page=Browse: Health: Vitamins & Supplements: Herbals: Selenium; s_cc=true; _cs_cvars={"1":["appName","browse-search-page"]}; _cs_id=9070dd93-eb45-acb3-b6f0-4be7eb5bb14e.1668590775.2.1668737498.1668737498.1.1702754775505; _cs_s=1.0.0.1668739298465; _gid=GA1.2.2010252333.1668737499; _pin_unauth=dWlkPU9EY3paRGczT0RndFpXRTNNaTAwTmprNExUa3hZbU10TnpNM09HTTFZbU0wWXpOag; AMCV_C4C6370453309C960A490D44@AdobeOrg=-1124106680|MCIDTS|19315|MCMID|90213565073727342430939047591393783286|MCAID|NONE|MCOPTOUT-1668744696s|NONE|MCAAMLH-1669342296|7|MCAAMB-1669342296|j8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI|MCSYNCSOP|411-19322|vVersion|5.2.0; _gat=1; __gpi=UID=00000b7d15788d1b:T=1668590774:RT=1668737501:S=ALNI_MaX74K8tQGKxpooCnfpg_ACNx2Wbw; wmt.c=0; seqnum=2; xpm=1+1668737491+UXumcqH8VGJvd_8jF3BM8M~+0; TS0196c61b=01cb3df500d7715063fab3bb4831778218717dd8840a9926bbe59ceb9a512454af7b3f66ec7d57a200db609b29bc05e2fd986c1e92; TS017d5bf6=01cb3df500d7715063fab3bb4831778218717dd8840a9926bbe59ceb9a512454af7b3f66ec7d57a200db609b29bc05e2fd986c1e92; TS01170c9f=01cb3df500d7715063fab3bb4831778218717dd8840a9926bbe59ceb9a512454af7b3f66ec7d57a200db609b29bc05e2fd986c1e92; bm_sv=009E573BC361DA5585F8F248D0BC8D98~YAAQMGdNaJrk7n6EAQAAl+qBiBEGwUybkZqzGUWz7ZP5gLCeDxxt3QfTZxIvlQuAZzwPiS8/Z5iu4MPy8+cbKuvuvn1ch6UTGsrfzL+JRqf+VK7DLHHSBTRDVlCB4Vqgs1VaavV7XcRYQsxJPbh4rsLFT7W/UITPeRlGCvnm/aX00Rdkv66qhFmenYJvsmPaZBqzWBDeVyNPY/SniCYCIbRkhF1Hdr/wdFEguE0bL8GZWiEMIUe9pBKNfwj2kC6XUw==~1'
    final_cookies = final_cookies_US if country == "US" else final_cookies_CA
    data = read_src(csv_path)
    n = 0
    headers = {
        'cookie': final_cookies,
        'user-agent': get_ua(),
        'upgrade-insecure-requests': '1'
    }

    pbar = tqdm(total=len(data))
    while n < len(data):

        link = data[n][0]
        print(link)
        sp = requests.session().get(link, headers=headers)
        print(sp.text)
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
            text = soup.find('script', attrs={"id": "__NEXT_DATA__"}).string
            soup = json.loads(text)
            table1 = get_info(link, table1, soup)
        except BaseException as e:
            print(e)
            table1.append([link, '-', '-', '-'])
        n += 1
        pbar.update(1)
    pbar.close()
    return table1


def main(country):
    date = datetime.today().strftime("%Y%m%d")
    table1 = []
    process(table1, country)
    print(table1)
    csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Walmart爬虫\Walmart_PriceOutput_' + \
        date + '_' + country + '.csv'
    table1.insert(0, ['Link', 'color', 'price', 'stock'])
    with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerows(table1)
    # mapping_sku(
    #     csv_path1,
    #     r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Walmart爬虫\SKU_Mapping.csv')


def construct_link(id_str: str, country: str) -> str:
    if id_str.startswith('https://'):
        return id_str
    elif country == "US":
        return f"https://www.walmart.com/ip/seort/{id_str}"
    elif country == "CA":
        return f"https://www.walmart.ca/en/ip/seort/{id_str}"
    else:
        return "Invalid country code. Please specify either 'US' or 'CA'."

def is_verify_identity_page(html_content):
   soup = BeautifulSoup(html_content, 'html.parser')
   if soup.title and soup.title.string:
       if 'Verify Your Identity' in soup.title.string:
           return True
   return False
if __name__ == '__main__':
    sku_list_file = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/Walmart爬虫/!运营维护_Walmart_SKU爬虫清单.csv'
    # 读取 Excel 文件中的数据
    df_all = pd.read_csv(sku_list_file)
    cookie1 = 'wm_route_based_language=en-CA; WM_SEC.AUTH_TOKEN=MTAyOTYyMDE4%2FDCHVSX44M2Ac9b7d%2FH4WIo2tau4JePgM8dU0HTkHeJK%2BWfGXDae3pYcrZGEHpU3XoUEpgxpzRr0LB%2B1tfVEdZv5xCaxAH%2BkXkp5BeJdGL8ErZq2XC5iMtGmMw%2B0ZGBij8OFN4dileb20bpDLeCIlSFd%2FHsc7bnSe4%2BTLU2zbj3TyuMuiwZmVlOiBvlWU0UbN4WEXbvtG5oQ2MZpCA1AAsOk2sYie0K%2BNPvZ1rS1Lxbb%2FSoGFgAYL9DGZ8K45WCXDCcb9mgycy9jtT1uIyOBHbOE%2Be7jBYTszK01uCYA1sZFGaB9kntB3PgAHPzZzyY7C175%2BJ1qPZicRTHBBvRQ16SAaqh2LcogVefvkCExlINpQy5WQokSTaQP%2Fp2pTRCrMbq55JFzIJtABSkbOixHYUr1eX9YGQ0laieVMoEr348%3D; auth=MTAyOTYyMDE4%2FDCHVSX44M2Ac9b7d%2FH4WIo2tau4JePgM8dU0HTkHeJK%2BWfGXDae3pYcrZGEHpU3XoUEpgxpzRr0LB%2B1tfVEdZv5xCaxAH%2BkXkp5BeJdGL8ErZq2XC5iMtGmMw%2B0ZGBij8OFN4dileb20bpDLeCIlSFd%2FHsc7bnSe4%2BTLU2zbj3TyuMuiwZmVlOiBvlWU0UbN4WEXbvtG5oQ2MZpCA1AAsOk2sYie0K%2BNPvZ1rS1Lxbb%2FSoGFgAYL9DGZ8K45WCXDCcb9mgycy9jtT1uIyOBHbOE%2Be7jBYTszK01uCYA1sZFGaB9kntB3PgAHPzZzyY7C175%2BJ1qPZicRTHBBvRQ16SAaqh2LcogVefvkCExlINpQy5WQokSTaQP%2Fp2pTRCrMbq55JFzIJtABSkbOixHYUr1eX9YGQ0laieVMoEr348%3D; DYN_USER_ID=841d685d-47b1-474c-8ff9-720ae4de3c8f; LT=1703577783391; WM.USER_STATE=GUEST%7CGuest; type=GUEST%7CGuest; ACID=841d685d-47b1-474c-8ff9-720ae4de3c8f; locDataV3=eyJwaWNrdXBTdG9yZSI6eyJhZGRyZXNzTGluZU9uZSI6IjgwMCBNYXRoZXNvbiBCbHZkIFciLCJjaXR5IjoiTWlzc2lzc2F1Z2EiLCJzdGF0ZU9yUHJvdmluY2VDb2RlIjoiT04iLCJjb3VudHJ5Q29kZSI6IkNBIiwicG9zdGFsQ29kZSI6Ikw1ViAyTjYiLCJzdG9yZUlkIjoiMTA2MSIsImRpc3BsYXlOYW1lIjoiSEVBUlRMQU5ELCBNSVNTSVNTQVVHQSxPTiIsImdlb1BvaW50Ijp7ImxhdGl0dWRlIjo0My42MDY5ODEsImxvbmdpdHVkZSI6LTc5LjY5MjU0Nn0sImFjY2Vzc1BvaW50SWQiOiJhZTg5Y2E1MS02YjU5LTRlYTctODgxZS01YTMyNzgwZDAwZWIiLCJmdWxmaWxsbWVudFN0b3JlSWQiOiIxMDYxIiwicHJpY2luZ1N0b3JlSWQiOiIxMDYxIiwiZnVsZmlsbG1lbnRPcHRpb24iOiJQSUNLVVAiLCJmdWxmaWxsbWVudFR5cGUiOiJJTlNUT1JFX1BJQ0tVUCJ9LCJzaGlwcGluZyI6eyJwb3N0YWxDb2RlIjoiTDVWIDJONiIsImNpdHkiOiJNaXNzaXNzYXVnYSIsInN0YXRlT3JQcm92aW5jZUNvZGUiOiJPTiIsImNvdW50cnlDb2RlIjoiQ0EiLCJsYXRpdHVkZSI6NDMuNjA2OTgxLCJsb25naXR1ZGUiOi03OS42OTI1NDYsImlzR2lmdEFkZHJlc3MiOmZhbHNlfSwiaW50ZW50IjoiUElDS1VQIiwiaXNFeHBsaWNpdEludGVudCI6ZmFsc2UsInZhbGlkYXRlS2V5IjoicHJvZDp2Mjo4NDFkNjg1ZC00N2IxLTQ3NGMtOGZmOS03MjBhZTRkZTNjOGYifQ%3D%3D; hasLocData=1; sizeID=99n0nfqpa22ah78cenljre4jkl; userAppVersion=main-1.73.2-fa21230-1221T2214; vtc=bT0XcYKAhaPCKc50voKu9Q; bstc=bT0XcYKAhaPCKc50voKu9Q; xpa=0yfou|9aQP5|ZqI6v; exp-ck=0yfou19aQP51ZqI6ve; locale_ab=true; ca-glass-web=true; uxcon=enforce=false&p13n=true&ads=true&createdAt=1703577784264&modifiedAt=; walmart.nearestPostalCode=L5V2N6; walmart.nearestLatLng="43.60822,-79.69387"; userSegment=50-percent; ak_bmsc=C1B015FE99AEB0E1EC07A437AF6F4972~000000000000000000000000000000~YAAQbqrBF37EPY+MAQAAGRAmpRaHRHovRHDZhkpvsUQxXZ3SEzeLlQItnnyGNtr0OKsqOyNpN+4CxR4e0NYTriItLjstqmjcDP3q+4QEUu4tysXqWqQ50tV/n+xa5ca2v4sFkAsgCrmMiBk5S0IV2D4MJBnwBRLH1aM0ex27ds0GDwb+02p8s3p5DrZg3ECEdnqrJkU7UMDSu4JAblOpytE9q0iTQj6ONGLkj8gT/Zruuoazk6U3EWXbmbhqdVPx9mG3/rvLeFI+Tr+54CEf9CzlhkpfnFUocEuIKfMUlxfHa/EldvaoeGs54hst+Go8yqsODu7UNte8M+PjyGDa3I6y369PwHXCvkdX7JZjd14NgFv3h8bEKUMQda3AvpZxJSyancdwkfO/DA==; _astc=fef772bc414c866aab4cd27ae70ae5af; adblocked=false; pxcts=32bfe7a7-a3c5-11ee-9c62-4e93ec9e2760; _pxvid=32bfd7b6-a3c5-11ee-9c62-f3741d79b4d0; btc=bT0XcYKAhaPCKc50voKu9Q; bsc=TSvVrC_7pWg-Z5uyE4h7SM; b30msc=bT0XcYKAhaPCKc50voKu9Q; _gcl_au=1.1.13640082.1703577785; _ga_D2P3FM55BM=GS1.1.1703577785.1.0.1703577785.0.0.0; _ga_N1HN887KY7=GS1.1.1703577785.1.0.1703577785.60.0.0; _ga=GA1.2.2010180006.1703577785; _gid=GA1.2.1659418737.1703577786; _gat_gtag_UA_25229943_4=1; test_cookie=CheckForPermission; _px3=344da69ddef393b3fdb0e3c13b2cad0ae0f9e136ec342e495f68f766ca2de7a9:kCRjFQ2uQF3RwjKAOSCuwOknGHi9Ex+HoTGEp47Qq+7Ck+8q2W0Qlqzgh5xK4u9h2GGjZBKVbI9NcKw+9IxBoA==:1000:QZNV8sxKi2BqUXoEZCi8TfLfXT7du+HRWdtb9JPV1a/Q8vXa2tXL2MLCmzcE/3S82htSF1fYNHIeXjk6FFNNdtXAZQvycuz8C4HyYMaMd3Fv2C5cI/GNgpRrGw62R3MK8/lT5OUwzIFEI5JA4rdLKIHy2GNrUGHq3TV/x2OxJEiFqhdhdZ2hHmAG6cmNyN5a8q7u9RP8ZpPIG1DXuLsgZJlcRKMNID8pSao5aExBLGo=; _pxde=c4806d0707064491cec52bd49ee193bc1e00ecde554da5def917f2769b9f1b36:eyJ0aW1lc3RhbXAiOjE3MDM1Nzc3ODYwMDh9; _tap_path=/rum.gif; uid=4326f191-cf7e-4286-a0f2-089404de7292; seqnum=2; xpm=1%2B1703577783%2BbT0XcYKAhaPCKc50voKu9Q~%2B0; TS010110a1=011b2a171ee67f2fd600ab92dd1673be4a6b1f5d675fe98587e69bc5e74ae521627a2e8c0d0c686d27327458f8bdbb331d09f8131e; TS01ea8d4c=011b2a171ee67f2fd600ab92dd1673be4a6b1f5d675fe98587e69bc5e74ae521627a2e8c0d0c686d27327458f8bdbb331d09f8131e; TS0180da25=011b2a171ee67f2fd600ab92dd1673be4a6b1f5d675fe98587e69bc5e74ae521627a2e8c0d0c686d27327458f8bdbb331d09f8131e; TSe62c5f0d027=08e4fd32c6ab20003bf23baf50af81a8c07fede454d2962063dafbed2498a998f727697878a759f40818ed5a4811300042ad2365551151ea8b7b803f9e78d6d498bb369b0fc84791d633186c343826cd1a592f7cad349e45ff110bf6ec3b3a99; bm_sv=F0EF276C1A0832156CBEAA828AF23283~YAAQbqrBF8bEPY+MAQAAqBompRbV0tZJobVkCsJzkXZbtuAltP8AokV+SmzUQn7Xzk8X/nGiPnn2Q8MviysADWgUCYir33kg5zC/0jBRRxEDnH4z0GwLbXtNaYIyHpQP8aSd7nqWfSsscp+363wA90PGoQv5nn+mR0RC9fzHxjqQchyA9JBsar+X/iucdIrPS7JlfV+8iSJQ+h5/8aP4epiGceLB5R+D+HtHKesKOuLBN+EN5s1LSQJBH25epVmR~1; criteo_thirdpartyuserid=jf2VtVwA6AS0ubdQLmC1yZ9hpAkdX0W4; _tap-criteo=1703577786334:1703577787158:1; _tap-bing=1703577787157:0:1; MUID=082FC4947BA1612A3A7CD7677ADB6017; MR=0'
    cookie2 = 'userSegment=50-percent; _gid=GA1.2.1030814445.1703470810; _gcl_au=1.1.749918562.1703470810; _scid=10c1a671-e68d-4002-a564-095657492031; _tt_enable_cookie=1; _ttp=agMREU8ReQT1pPARI2lvM_7xp-a; s_ecid=MCMID%7C43975267453999037783475405469343118990; _pxvid=216583cf-a2cc-11ee-a160-4053c1534b2a; _sctr=1%7C1703433600000; localStoreInfo=eyJwb3N0YWxDb2RlIjoiTDVWMk42IiwibG9jYWxTdG9yZUlkIjoiMTA2MSIsInNlbGVjdGVkU3RvcmVJZCI6IjEwNjEiLCJzZWxlY3RlZFN0b3JlTmFtZSI6IkhlYXJ0bGFuZCBTdXBlcmNlbnRyZSIsImZ1bGZpbGxtZW50U3RvcmVJZCI6IjEwNjEiLCJmdWxmaWxsbWVudFR5cGUiOiJJTlNUT1JFX1BJQ0tVUCIsImFzc2lnbmVkRmFsbGJhY2siOnRydWV9; deliveryCatchment=1061; walmart.nearestPostalCode=L5V2N6; walmart.shippingPostalCode=L5V2N6; defaultNearestStoreId=1061; wmt.c=0; vtc=Wqx7Z79UiJVRukDB_iHxhg; WM_SEC.AUTH_TOKEN=MTAyOTYyMDE4l0P5YVLOvkkhTvkiY0BvzRk7ECiHp0gHL0WkdeslTQ4qk7pOu8K0X3pqTcBVUuGLXNIkwD7wSkNkGZ8IhpK%2BVDzvRFAoBVgQLR1oSrWIUnrO2TMcPXQj2nw8BBkmeGrZj8OFN4dileb20bpDLeCIlSFd%2FHsc7bnSe4%2BTLU2zbj2DMsZ19M1YYghFsdjJa7Ym0aG7eLU2TvoxF0wbUEKnmpmgQm0UjlFf2hseVBXgDnPb%2FSoGFgAYL9DGZ8K45WCXDCcb9mgycy9jtT1uIyOBHTsP9SsK1TAFW6TqywsJBoX%2B2257sBQXFfMAsPpEuwMN4ZzAjAYE4zZRQ%2FyFeMOBB%2FwMPevmgEbUaDOUeZP41Et33KM%2Fu2bNNeBD3SdUIyNhF%2FU6NSXPvbA3XR%2Bhe06bT0r1eX9YGQ0laieVMoEr348%3D; auth=MTAyOTYyMDE4l0P5YVLOvkkhTvkiY0BvzRk7ECiHp0gHL0WkdeslTQ4qk7pOu8K0X3pqTcBVUuGLXNIkwD7wSkNkGZ8IhpK%2BVDzvRFAoBVgQLR1oSrWIUnrO2TMcPXQj2nw8BBkmeGrZj8OFN4dileb20bpDLeCIlSFd%2FHsc7bnSe4%2BTLU2zbj2DMsZ19M1YYghFsdjJa7Ym0aG7eLU2TvoxF0wbUEKnmpmgQm0UjlFf2hseVBXgDnPb%2FSoGFgAYL9DGZ8K45WCXDCcb9mgycy9jtT1uIyOBHTsP9SsK1TAFW6TqywsJBoX%2B2257sBQXFfMAsPpEuwMN4ZzAjAYE4zZRQ%2FyFeMOBB%2FwMPevmgEbUaDOUeZP41Et33KM%2Fu2bNNeBD3SdUIyNhF%2FU6NSXPvbA3XR%2Bhe06bT0r1eX9YGQ0laieVMoEr348%3D; DYN_USER_ID=000e26f2-539c-4c3e-9ea8-d8c65fa728eb; ACID=000e26f2-539c-4c3e-9ea8-d8c65fa728eb; locDataV3=eyJwaWNrdXBTdG9yZSI6eyJhZGRyZXNzTGluZU9uZSI6IjgwMCBNYXRoZXNvbiBCbHZkIFciLCJjaXR5IjoiTWlzc2lzc2F1Z2EiLCJzdGF0ZU9yUHJvdmluY2VDb2RlIjoiT04iLCJjb3VudHJ5Q29kZSI6IkNBIiwicG9zdGFsQ29kZSI6Ikw1ViAyTjYiLCJzdG9yZUlkIjoiMTA2MSIsImRpc3BsYXlOYW1lIjoiSEVBUlRMQU5ELCBNSVNTSVNTQVVHQSxPTiIsImdlb1BvaW50Ijp7ImxhdGl0dWRlIjo0My42MDY5ODEsImxvbmdpdHVkZSI6LTc5LjY5MjU0Nn0sImFjY2Vzc1BvaW50SWQiOiJhZTg5Y2E1MS02YjU5LTRlYTctODgxZS01YTMyNzgwZDAwZWIiLCJmdWxmaWxsbWVudFN0b3JlSWQiOiIxMDYxIiwicHJpY2luZ1N0b3JlSWQiOiIxMDYxIiwiZnVsZmlsbG1lbnRPcHRpb24iOiJQSUNLVVAiLCJmdWxmaWxsbWVudFR5cGUiOiJJTlNUT1JFX1BJQ0tVUCJ9LCJzaGlwcGluZyI6eyJwb3N0YWxDb2RlIjoiTDVWIDJONiIsImNpdHkiOiJNaXNzaXNzYXVnYSIsInN0YXRlT3JQcm92aW5jZUNvZGUiOiJPTiIsImNvdW50cnlDb2RlIjoiQ0EiLCJsYXRpdHVkZSI6NDMuNjA2OTgxLCJsb25naXR1ZGUiOi03OS42OTI1NDYsImlzR2lmdEFkZHJlc3MiOmZhbHNlfSwiaW50ZW50IjoiUElDS1VQIiwiaXNFeHBsaWNpdEludGVudCI6ZmFsc2UsInZhbGlkYXRlS2V5IjoicHJvZDp2MjowMDBlMjZmMi01MzljLTRjM2UtOWVhOC1kOGM2NWZhNzI4ZWIifQ%3D%3D; hasLocData=1; sizeID=j0vk7khh5gof0e3an6hdcn5bip; userAppVersion=main-1.73.2-fa21230-1221T2214; walmart.nearestLatLng="43.60822,-79.69387"; adblocked=false; criteo_thirdpartyuserid=; QuantumMetricUserID=d6eaefd3eb1a7407dcbe031c59cb4037; cartId=795b46ac-b585-4f31-b228-6203a9066f13; uxcon=enforce=false&p13n=true&ads=true&createdAt=1703471225122&modifiedAt=1703471350460; wm_route_based_language=en-CA; bstc=QPvLyfiVu-6jeHlaBx87R0; xpa=0yfou|9aQP5|ZqI6v; exp-ck=0yfou19aQP51ZqI6ve; ak_bmsc=A6C4B6743498E6D0B42EB0576BBDF077~000000000000000000000000000000~YAAQbqrBF+H7O4+MAQAAeF3UpBYwhk5JSz6xVBG0++Jsm5UM5VMHypqrUlQzypqynR7NSeKRiEcAd3PjIJTiOzhbi0BCKVBSggD9OjLOWIo7R2QLjJDm1KM/s/jJVwztAr/ddSClG9Q/FtCyzai7zJQoRqBv8n6LsYtSEwbgbldyakDaUizW++oxPw2vbPWUGOO0ffUP/irbMYDCL7l3WayP7TWNyngX9q7WchJAc8aYS9+yKMgvLnApBSlbmQCABMx6gLHGhhRujwbSEP9BOypZKKE9c204IMYSObQC58NI5xnMPkv9JV9MjGEcZJTVgDuU1gTZHrFTMx/teapkedCks3y+2kkjH3xK9YWV/Ye9cvzmGD61qj9zCCl16h8RbcvGiFOBDYIy9w==; _astc=fef772bc414c866aab4cd27ae70ae5af; pxcts=bb90c8a1-a3b8-11ee-8430-e748cf80768c; QuantumMetricSessionID=ca23882eeef654c19d5d6454c252f254; xpm=1%2B1703572429%2BWqx7Z79UiJVRukDB_iHxhg~%2B0; s_visit=1; gpv_Page=Bot%20Protection%20Page; kndctr_C4C6370453309C960A490D44_AdobeOrg_identity=CiY0Mzk3NTI2NzQ1Mzk5OTAzNzc4MzQ3NTQwNTQ2OTM0MzExODk5MFIRCPOZl_bJMRgBKgRKUE4zMAPwAfe48qbKMQ==; kndctr_C4C6370453309C960A490D44_AdobeOrg_cluster=jpn3; AMCVS_C4C6370453309C960A490D44%40AdobeOrg=1; AMCV_C4C6370453309C960A490D44%40AdobeOrg=-1124106680%7CMCIDTS%7C19717%7CMCMID%7C43975267453999037783475405469343118990%7CMCAID%7CNONE%7CMCOPTOUT-1703580170s%7CNONE%7CMCAAMLH-1704177770%7C11%7CMCAAMB-1704177770%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCSYNCSOP%7C411-19724%7CvVersion%7C5.2.0; s_cc=true;TS01ea8d4c=011b2a171e3924ab0be43bfbf8fa67507fb66b42a075866b60b24f33d8b449f9fb40abe1631f18ebb7d4513006fc23dd921d0f2e08; TS0180da25=011b2a171e3924ab0be43bfbf8fa67507fb66b42a075866b60b24f33d8b449f9fb40abe1631f18ebb7d4513006fc23dd921d0f2e08; _ga=GA1.2.87968351.1703470810; seqnum=20; TS010110a1=011b2a171eb1bb6153b69ae79aded0d5833a93dc715d83973453cb4f941e33416d956a4b7a92ef8c6e1b16ede06e706834da1505bd; TSe62c5f0d027=08e4fd32c6ab20007c1db116e5d7af3faf090fa09c02196ae75c6e43d49b01fc0897f91b4ab3ab9208335fc5ff113000ef637aad60787f91b6b20071a8ac1a2eaf2fbec5666febec7ca75d56c780c80fa4d5132c9a48bc374a0e7473acdc858e; _ga_D2P3FM55BM=GS1.1.1703572431.7.1.1703575bm_sv=03B66175DD2334B4EFB7652733ABD417~YAAQbqrBF7omPY+MAQAAh/UKpRYu254ZWLPHfv/SpnS6TrGrLgl5sx4529+W1rYrGA7Y+JR/m/UnFikBDXLH/r3ZREvzLdheqvT4yhlK+EnJ2infKD+aXtQe5nujF9gb1gffXGMznXYHOFWYjloWMk/sI6J8sUQC4ESF+MRXCdZbu0MNPSXEy7HeeSGdGEsJmfOuzwf5h3/dLtJR6tGMZ2uJB9ZZ2UI28pkuFpqMiw4xM1JOyv+Fwv4tBSM93627xQ==~1; s_gnr=1703576008459-Repeat; _uetsid=fd3bc100a3b911eea68e874ade43cfd0; _uetvid=20ad5320a2cc11ee85c17b0b5d0b33f6; _scid_r=10c1a671-e68d-4002-a564-095657492031; _ga_N1HN887KY7=GS1.2.1703572431.7.1.1703576009.33.0.0; _px3=ba6edf26829fb490bb80547bcdb9b074bc82e21e66758ec5a2f0d1c0023b1229:MaTZqG+6cXnvcGtqW2NOKJJFyGcYlSItjpt/4cxV5ejBeTBvifNLAUln4RROTPXLJ3s+7ypSJn9+I4M1duJv5A==:1000:Pjx9i78S676lcHfHOVU0apBHkuiBkoPlYBA1m9P1FaijXJld/2850pWmYhuFpnUXNOLkyBAaHtHd2uJQ6b1DbWnZbGpoOr+iMm7je3PsyCznU7ruxfEPT7RmfLsye4zOHyEL3in6Oz1n/W372NXP9jNFj4Hnn4xvINfkaYLiRytkeSJtJFTKQdLDNexANhXzxT5G/nDdtsf/5y+7jQfPkzXeGpQP6DvQiVWcwC2AAlw=; _pxde=2f7157bab838685e9f43355172a7e23af4d79e1e24aaaba57f26abc1df052fc2:eyJ0aW1lc3RhbXAiOjE3MDM1NzYzODI4NDl9'
    headers = {
        'cookie': 'userAppVersion=main-1.73.2-fa21230-1221T2214',
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
    }
        # 遍历 DataFrame，为每行数据发起请求
    for _, row in df_all.iterrows():
        print(row['Platform_SKU'])
        url = construct_link(row['Platform_SKU'], row['Country'])
        # print(requests.get('http://myip.ipip.net/', proxies=proxies).text)
        for i in range(3):
            try:
                sp = requests.get(url, headers=headers,proxies=proxies)
                break  # if the request is successful, the loop will break otherwise it will continue until i < 3
            except Exception as e:
                print(f"SSL Error occurred: {e}")
                print("Retrying...")
                sleep(2)  # optional delay before retrying, you may remove it
        if is_verify_identity_page(sp.content):
            print("遭遇验证，cookie被封")
        else:
            # 使用 BeautifulSoup 解析HTML
            soup = BeautifulSoup(sp.content, 'html.parser')

            # 寻找具有特定id的 <script> 标签
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if script_tag is not None:
                json_text = script_tag.string

                # 解析 JSON 文本为 Python 数据结构
                data = json.loads(json_text)
                product = data['props']['pageProps']['initialData']['data']['product']
                if product:
                    if 'variantProductIdMap' in product and product['variantProductIdMap']:
                        variantProductIdMap = product['variantProductIdMap']
                        for i in variantProductIdMap:
                            color = i
                            id = variantProductIdMap[i]
                            if id == row['Platform_SKU']:
                                option = product['variantsMap'][id]
                                price = option['priceInfo']['currentPrice']
                                avlib = option['availabilityStatus']
                                output = [id, color, price, avlib]
                                print(output)
                    elif 'priceInfo' in product and 'currentPrice' in product['priceInfo']:
                        if product['priceInfo']['currentPrice']:
                            price = product['priceInfo']['currentPrice']['price']
                        else:
                            price = None
                        output = [None, None, price, None]
                        print(output)
