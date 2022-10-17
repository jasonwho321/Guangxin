from xbot import print, sleep, web
from bs4 import BeautifulSoup
import csv
import json
from datetime import datetime
import requests


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


# def create_cookies():
#     # 打开网页并获取cookies
#     web_object = web.create('https://www.wayfair.com/furniture/sb0/gaming-chairs-c410369.html', 'cef',
#                             load_timeout=20, stop_if_timeout=True)
#     cookie_list = web_object.get_cookies()
#     cookies = ''
#     for cookie in cookie_list:
#         cookies = cookies + "; " + cookie['name'] + "=" + cookie['value']
#     web.close_all('cef')
#     headers = {
#         'cookie': cookies,
#         'user-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
#         'referer': 'https://www.wayfair.com/furniture/pdp/furgle-gaming-chair-vcts1083.html',
#         # 'cookie': 'CSNUtId=23e17d3a-6075-b5be-352a-432d710a2502; ExCSNUtId=23e17d3a-6075-b5be-352a-432d710a2502; vid=23e17d3a-6075-b5be-352a-432d710a2502; SFSID=b6198ed2962142502a3506f7210da359; canary=1; WFDC=DSM; serverUAInfo=%7B%22browser%22%3A%22Google%20Chrome%22%2C%22browserVersion%22%3A88.04324104%2C%22OS%22%3A%22Windows%22%2C%22OSVersion%22%3A%2210%22%2C%22isMobile%22%3Afalse%2C%22isTablet%22%3Afalse%2C%22isTouch%22%3Afalse%7D; __ssid=a1d0cfe445f83053367b76d391efb8f; IR_gbd=wayfair.com; rskxRunCookie=0; rCookie=t6dmfypqgyfqwjwycgro7okng64qcq; AppInterstitial=visit_date_1%3D2021-04-13; _ga=GA1.2.1655478551.1618327159; _gid=GA1.2.826818772.1618327159; TopNavCSSCachedByBrowser=true; CSN=g_countryCode%3DUS%26g_zip%3D67346; otx=I+F9OmB1t7m/FkgpBmYVAg==; categoryId=45974; CSNPersist=page_of_visit%3D47; IR_12051=1618327505620%7C0%7C1618327505620%7C%7C; lastRskxRun=1618327506200'
#         'upgrade-insecure-requests': '1'
#     }
#     return headers
#
# def session(url):
#     headers = create_cookies()
#     session = requests.session()
#     r = session.get(url=url,headers=headers)
#     print(r.content)

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


def not_bot(new_url):
    sp = web.create(new_url, 'cef', stop_if_timeout=True)
    try:
        sp.find_by_xpath(
            '/html/body//main//div[@class="PdpLayoutResponsive-top"]')
    except BaseException:
        print('正在绕过机器人检测')
        cookies_list = web.get_cookies(new_url)
        for cookie in cookies_list:
            for name in cookie:
                web.remove_cookie(new_url, name)
        web.close_all('cef')
        sp = web.create(new_url, 'cef', stop_if_timeout=True)
    finally:
        return sp


def get_info(sku, c_sku, link_ava, waymore_num, new_url, prop):
    sp = not_bot(new_url)

    list_pic = sp.find_all_by_xpath(
        '/html/body//main//div//ul/li[@class="ProductDetailImageCarousel-carouselItem"]')
    pic_num = len(list_pic)
    title = sp.find_all_by_xpath(
        '/html/body//div/main/div//div/header/h1[@class="pl-Heading pl-Heading--pageTitle pl-Box--defaultColor"]'
    )
    title = title[0].get_text()



    video_link = prop["mainCarousel"]["videos"]
    salePrice = sp.find_all_by_xpath(
        '/html/body//div/main/div//div//div//span[@font-size="5000"]'
    )
    salePrice = salePrice[0].get_text()
    link_list = []
    for i in video_link:
        link_list.append(i['sources'][0]['source'])
    try:
        sp.find_by_xpath(
            '/html/body//div/main/div//div/button[@class="OutOfStockOverlay OutOfStockOverlay--withAnchor"]'
        )

    except:
        is_out_of_stock = 'in_stock'
    else:
        is_out_of_stock = 'out_of_stock'

    output = [sku[0], c_sku, title, is_out_of_stock, salePrice]
    print('已获取 {} 的全部信息\n{}'.format(c_sku, output))
    return output
    # return [sku[0], c_sku, link_ava, waymore_num, is_out_of_stock,
    #         pic_num, len(link_list)] + link_list


def get_all_sku(sku, waymore_num, table1):

    sp = web.create(
        'https://www.wayfair.com/keyword.php?keyword=' +
        sku[0],
        'cef',
        stop_if_timeout=True)
    # sp = web.create('https://www.wayfair.com/keyword.php?keyword=TNFI1003',browser[0],stop_if_timeout=True)
    try:
        link_ava = "Y"
        soup = BeautifulSoup(sp.find_all_by_xpath(
            '/html/body/script')[-2].get_html(), 'lxml')
        text = soup.find_all('script', type='text/javascript')
        try:
            application = json.loads(text[-1].string[29:-1])["application"]
        except BaseException:
            application = json.loads(
                text[-1].string[29:-1])["temp-application"]
        prop = application["props"]
        categories = prop['options']['standardOptions']
        exceptions = prop['options']['exceptions']
        if len(categories) > 0:
            all_combination, cate_dict = get_all_combine(categories)

            # all_list = []
            url = web.get_active().get_url()
            for com in all_combination:
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
                    new_url = url + "&piid=" + piids
                    list1 = get_info(
                        sku, c_sku, link_ava, waymore_num, new_url, prop)
                    table1.append(list1)
        else:
            url = web.get_active().get_url()
            new_url = url
            list1 = get_info(
                sku, sku[0], link_ava, waymore_num, new_url, prop)
            table1.append(list1)
    except Exception:
        pic_num = '-'
        c_sku = '-'
        link_list = []
        link_ava = "N"
        is_out_of_stock = '-'
        list1 = [
            sku[0],
            c_sku,
            link_ava,
            waymore_num,
            is_out_of_stock,
            pic_num,
            len(link_list)] + link_list
        table1.append(list1)
    print('已获取{}所有sku信息'.format(sku[0]))
    return table1


def main(args):
    csv_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\sku_list.csv'
    data = read_src(csv_path)

    time = datetime.today().strftime("%Y%m%d")
    csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\PriceOutput_' + time + '.csv'

    table1 = []

    for sku in data:
        print("总体进度：{}/{}".format(data.index(sku), len(data)))
        sp = not_bot('https://www.wayfair.com/keyword.php?keyword=' +
                     sku[0])
        list_waymore = sp.find_all_by_xpath(
            '/html/body/div//section[@class="WaymoreModule"]')
        waymore_num = len(list_waymore)

        table1 = get_all_sku(sku, waymore_num, table1)
        web.close_all('cef')
    with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerows(table1)
    pass


if __name__ == '__main__':
    main(1)
