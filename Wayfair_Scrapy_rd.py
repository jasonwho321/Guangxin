# coding=utf-8
from Wayfair_Scrapy import *
from datetime import timedelta
link_ca = 'https://www.wayfair.ca/graphql'
link_us = 'https://www.wayfair.com/graphql'
csv_path_US = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_list_US.csv'
# csv_path_US = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_list1.csv'
csv_path_CA = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_list_CA.csv'
hash_us = 'fee62fb54d3d50424e02336d2dd8521b#7f637117145182d911071391763e37cc#77c656652c1c4814baceb2546e8c8ff9#cb05724590826da5c40607d51d79e079#08c5d1b7acbca00d37d05400adc9bee8#28c908fd3aa88467ecab787c0251bde9'
hash_ca = '9019b6158a2d29556a702eee5194ad24#7f637117145182d911071391763e37cc#77c656652c1c4814baceb2546e8c8ff9#cb05724590826da5c40607d51d79e079#08c5d1b7acbca00d37d05400adc9bee8#28c908fd3aa88467ecab787c0251bde9'

def stock_contrast(country):
    date = datetime.today().strftime("%Y%m%d")

    today_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Wayfair_PriceOutput_' + \
                date + '_' + country + '.csv'
    df_today = pd.read_csv(today_path)
    d = 1
    while True:
        try:
            last_date = datetime.today() - timedelta(days=d)
            last_date = last_date.strftime("%Y%m%d")
            last_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Wayfair_PriceOutput_' + \
                         last_date + '_' + country + '.csv'
            df_yesterday = pd.read_csv(last_path)
            break
        except:
            d+=1
            pass
    df_yesterday = df_yesterday.to_dict('list')
    df_map = dict(zip(df_yesterday['optionid'], df_yesterday['stock']))
    df_today['optionid'] = df_today['optionid'].str.strip()
    df_today['yes_stock'] = df_today['optionid'].map(df_map, na_action=None)
    df_today['stock_contrast'] = df_today.apply(stock_func,axis=1,args=('yes_stock','stock'))
    resum_list = df_today.loc[df_today['stock_contrast']=='Resumption']
    resum_list = resum_list['partner_number'].astype(str).tolist()
    out_list =df_today.loc[df_today['stock_contrast']=='Out_Of_Stock']
    out_list = out_list['partner_number'].astype(str).tolist()
    bot_push_text('今日{}断货SKU：\n  {}\n\n今日恢复供货SKU：\n  {}'.format(country,"\n  ".join(out_list),"\n  ".join(resum_list)),key='affe1d05-9b70-4206-b0f1-7d4b2bb96b16',mobile_list=['13672386923'])

def stock_func(df_today,yes_stock,stock):
    if df_today[yes_stock] == 'OUT_OF_STOCK' and df_today[stock] == 'IN_STOCK':
        return 'Resumption'
    elif df_today[yes_stock] == 'IN_STOCK' and df_today[stock] == 'OUT_OF_STOCK':
        return 'Out_Of_Stock'
    elif df_today[yes_stock] == 'OUT_OF_STOCK' and df_today[stock] == 'LOW_STOCK':
        return 'Resumption'
    elif df_today[yes_stock] == 'LOW_STOCK' and df_today[stock] == 'OUT_OF_STOCK':
        return 'Out_Of_Stock'
    else:
        return '-'

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
    try:
        return reduce(myfunc, lists)
    except:
        return []

def get_all_combine(categories):
    combine_list = []
    cate_dict = {}
    for category in categories:
        options = category['productOptions']
        categoryName = category['categoryName']
        list_by_cate = []
        for option in options:
            list_by_cate.append(option['optionId'])
            value = "{}:{}".format(categoryName, option['name'])
            cate_dict[str(option['optionId'])] = value
        combine_list.append(list_by_cate)

    all_list = lists_combination(combine_list)
    new_all_list = []
    if all_list:
        for combination in all_list:
            new_all_list.append(str(combination).split('|'))
    return new_all_list, cate_dict

def sort_list(list):
    exceptions = []
    for i in list:
        i.sort()
        new_i = []
        for g in i:
            new_i.append(str(g))
        exceptions.append(new_i)
    return exceptions

def getinfo(sku,optionId_list,link):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.54',
        'use-web-hash': 'true'
    }
    sp = requests.post(
        link,
        json={"variables":{"sku":sku,"additionalSkus":[sku],"selectedOptionsIds":optionId_list if optionId_list else []}},
        params={"hash":"e67bfa2d6bd7bcf6d8c351a68c4e18aa#77c656652c1c4814baceb2546e8c8ff9#cb05724590826da5c40607d51d79e079#28c908fd3aa88467ecab787c0251bde9"},
        headers=headers)
    content = sp.json()
    if content['data']['product']['optionMatchedProducts']:
        optionMatchedProduct = content['data']['product']['optionMatchedProducts'][0]
    else:
        return {'listPrice':'-','customerPrice':'-','stock':'-','sale_tag':'-'}
    try:
        if optionMatchedProduct['pricing']['listPrice'] is None:
            listPrice = '-'
        else:
            listPrice = optionMatchedProduct['pricing']['listPrice']['unitPrice']['value']
        customerPrice = optionMatchedProduct['pricing']['customerPrice']['unitPrice']['value']
        stock = optionMatchedProduct['inventory']['stockStatus']
        sale_tag = optionMatchedProduct['pricing']['priceBlockElements'][0]['appliedPromotion']
        return {'listPrice':listPrice,'customerPrice':customerPrice,'stock':stock,'sale_tag':sale_tag}
    except:
        return {'listPrice':'-','customerPrice':'-','stock':'-','sale_tag':'-'}

def getqpl(info_list,sku,country,lock=None):
    link = link_us if country == "US" else link_ca
    hash = hash_us if country == "US" else hash_ca
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.54',
        'use-web-hash': 'true'
    }
    sp = requests.post(
        link,
        json={"variables":{"sku":sku}},
        params={"hash":hash},
        headers=headers)
    content = sp.json()
    # with open(r'C:\Users\Admin\data.json', 'w', encoding='utf-8') as f:
    #     json.dump(content, f, ensure_ascii=False, indent=4)
    if 'errors' in content:
        info_dict = {"SKU":sku,"title":'-',"full_name":'-',"optionid":sku,"stock":'-',"customerPrice":'-',"listPrice":'-',"sale_tag":'-',"rate":'-',"reviews":'-',"url":'-'}
        if lock:
            lock.acquire()
            for key in info_dict:
                row = info_list[key]
                row.append(info_dict[key])
                info_list[key] = row
            lock.release()
        else:
            for key in info_dict:
                info_list[key].append(info_dict[key])
        return info_list
    items = content['data']['product']['crossSell']['items']
    if items:
        n=0
        item = items[n]
        while items[n]['sku'] != sku:
            n+=1
            if n == len(items):
                info_dict = {"SKU":sku,"title":'-',"full_name":'-',"optionid":sku,"stock":'-',"customerPrice":'-',"listPrice":'-',"sale_tag":'-',"rate":'-',"reviews":'-',"url":'-'}
                if lock:
                    lock.acquire()
                    for key in info_dict:
                        row = info_list[key]
                        row.append(info_dict[key])
                        info_list[key] = row
                    lock.release()
                else:
                    for key in info_dict:
                        info_list[key].append(info_dict[key])
                return info_list
            item = items[n]
        customerReviews = item['customerReviews']
        optionExceptions = item['optionExceptions']
        optionCategories = item['options']['optionCategories']
        title = item['name']
        url = item['url']


    else:
        info_dict = {"SKU":sku,"title":'-',"full_name":'-',"optionid":sku,"stock":'-',"customerPrice":'-',"listPrice":'-',"sale_tag":'-',"rate":'-',"reviews":'-',"url":'-'}
        if lock:
            lock.acquire()
            for key in info_dict:
                row = info_list[key]
                row.append(info_dict[key])
                info_list[key] = row
            lock.release()
        else:
            for key in info_dict:
                info_list[key].append(info_dict[key])
        return info_list

    rate = customerReviews['averageRatingValue']
    reviews = customerReviews['ratingCount']

    new_all_list, cate_dict = get_all_combine(optionCategories)
    new_all_list = sort_list(new_all_list)
    exceptions = sort_list(optionExceptions)

    if new_all_list:
        for combin in new_all_list:
            if combin not in exceptions:
                info_dict = getinfo(sku,combin,link)
                full_name_list = []
                for c in combin:
                    full_name_list.append(cate_dict[c])
                info_dict['full_name'] = '&'.join(full_name_list)
                info_dict['optionid'] = '&'.join(combin)

                info_dict['SKU'] = sku
                info_dict['rate'] = rate
                info_dict['reviews'] = reviews
                info_dict['title'] = title
                info_dict['url'] = url+'?&piid='+ info_dict['optionid']
                if lock:
                    lock.acquire()
                    for key in info_dict:
                        row = info_list[key]
                        row.append(info_dict[key])
                        info_list[key] = row
                    lock.release()
                else:
                    for key in info_dict:
                        info_list[key].append(info_dict[key])

    else:
        info_dict = getinfo(sku,[],link)
        info_dict['full_name'] = title
        info_dict['optionid'] = sku
        info_dict['SKU'] = sku
        info_dict['rate'] = rate
        info_dict['reviews'] = reviews
        info_dict['title'] = title
        info_dict['url'] = url+'?&piid='+ info_dict['optionid']
        if lock:
            lock.acquire()
            for key in info_dict:
                row = info_list[key]
                row.append(info_dict[key])
                info_list[key] = row
            lock.release()
        else:
            for key in info_dict:
                info_list[key].append(info_dict[key])
    return info_list

def process(sku, miss_list, dict1,country,lock):
    try:
        dict1 = getqpl(dict1, sku[0],country,lock)
    except:
        traceback.print_exc()
        miss_list.append(sku)
    return dict1,miss_list

if __name__ == '__main__':
    country_list = ["US", "CA"]
    s = time()
    for country in country_list:
        csv_path = csv_path_US if country == "US" else csv_path_CA
        data = read_src(csv_path)
        lenth = len(data)
        date = datetime.today().strftime("%Y%m%d")
        info_list = {"SKU": [], "title": [], "full_name": [], "optionid": [], "stock": [], "customerPrice": [],
                     "listPrice": [], "sale_tag": [], "rate": [], "reviews": [], "url": []}
        manager = Manager()
        dict1 = manager.dict()
        dict1.update(info_list)
        lock = manager.Lock()
        miss_list = manager.list()
        pool_num = cpu_count()

        pbar = tqdm(total=lenth)
        update = lambda *args: pbar.update(1)
        workers = Pool(pool_num)
        for sku in data:
            workers.apply_async(
                process, (sku, miss_list, dict1,country,lock), callback=update)
        workers.close()
        workers.join()
        data = miss_list
        while data is not None:
            miss_list = []
            for sku in data:
                print('{},{}/{}'.format(sku[0],data.index(sku),len(data)))
                try:
                    print(len(dict1["SKU"]))
                    dict1 = getqpl(dict1,sku[0],country)
                except:
                    miss_list.append(sku)
            if miss_list:
                data = miss_list
                sleep(600)
            else:
                data = None
        csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Wayfair_PriceOutput_' + \
            date + '_' + country + '.csv'
        df = pd.DataFrame(data=dict1.copy())
        df.to_csv(csv_path1,index_label=None,index=False)
        mapping_sku(
            csv_path1,
            r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_Mapping_{}.csv'.format(country),
            'optionid', 'partner_number')
        stock_contrast(country)
    e = time()
    bot_push_text('{}\n总用时：{}s'.format(os.path.basename(__file__), strftime("%H:%M:%S", gmtime(e - s))))