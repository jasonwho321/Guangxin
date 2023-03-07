from Wayfair_Scrapy import *
from Overstock_Scrapy import mapping_sku

csv_path = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Amazon爬虫\SKU_list_{}.csv'
country_list = ['CA']
cookie_US = 'session-id=132-3025649-3531568; i18n-prefs=USD; skin=noskin; ubid-main=134-8454797-8017726; session-id-time=2082787201l; lc-main=en_US; session-token="g7reWoH9uzHvllMfIcP2v3qUY2nR2izGuac/GUnKvTleF3ArXEOFyRmnWVAxRheED56LBEQiacReWM5U4ng6WXOxQc1FKaUnm+fcU1n7XO0DLXdk0PbP7zaLrHSp6UeBKw1LtOy2TBBp9/zoy/U2MgNcN7k5QexvEHSzYCNKY45wSrDEnxAGLmG26XmSFYsi1ezsJLW4t0Uu125u8VQjNMa85RsnahVsM3HxtUYLBHY="; csm-hit=adb:adblk_no&t:1670316849288&tb:N3QVYE3M3DRFENCJ4GR8+s-1EHXY0SC7EJSKTQCXY31|1670316849288'
cookie_CA = 'session-id=133-1803103-9621123; session-id-time=2082787201l; i18n-prefs=CAD; ubid-acbca=132-6933899-9953406; lc-acbca=en_CA; session-token=jVdpSzUfnqjtqUPv6d8Envg9p5/bwtm455HywbszBKm90yceI/8IJs1xAW2buJhxSK/Ob4Giq/WpmmUcIzpSpOVPHGyFnbzBxwXDhyZHegrW+oiXUjtI/8J+pwPwXk0Tgyn9SEs5L0P26dDfILQSAWf27k7RWi+G5Rxp2JCNAxUAgyXCD8q0AdhqA2us8Ol4S4Z2BCuxxiXR0XFLBG+BqHOEyjALANNhqXgY3Va5Cr0; csm-hit=tb:26YA1MJ9CNY24ZJ7RNDP+s-S0ZN8RHKF69T313K6FVE|1677750095060&t:1677750095060&adb:adblk_no'

if __name__ == '__main__':
    s = time()
    date = datetime.today().strftime("%Y%m%d")
    for country in country_list:
        cookie = cookie_US if country == 'US' else cookie_CA
        data = read_src(csv_path.format(country))
        ua = get_ua()
        dict1 = {
            'Asin': [],
            'Sale_price': [],
            'title': [],
            'rating': [],
            'star': []}
        for i in tqdm(range(len(data))):
            headers = {
                'cookie': cookie,
                'user-agent': get_ua(),
                'upgrade-insecure-requests': '1'
            }
            asin = data[i][0]
            url = 'https://www.amazon.{}/gp/product/ajax/ref=auto_load_aod?asin={}&experienceId=aodAjaxMain'.format('com' if country == 'US' else 'ca',
                                                                                                                    asin)
            sp = requests.get(url, headers=headers)
            content = sp.content
            soup = BeautifulSoup(content, "html.parser")
            limit = 0
            print(soup.title)
            while soup.title is not None:
                print(limit)
                if limit > 3:
                    break
                else:
                    limit += 1
                headers = {
                    'cookie': cookie,
                    'user-agent': get_ua(),
                    'upgrade-insecure-requests': '1'
                }
                asin = data[i][0]
                url = 'https://www.amazon.com/gp/product/ajax/ref=auto_load_aod?asin={}&experienceId=aodAjaxMain'.format('com' if country == 'US' else 'ca',
                                                                                                                        asin)
                sp = requests.get(url, headers=headers)
                content = sp.content
                soup = BeautifulSoup(content, "html.parser")
            if soup.find_all('span', class_='a-offscreen'):
                sale_price = soup.find_all(
                    'span', class_='a-offscreen')[0].text.strip()
            else:
                sale_price = '-'
            if soup.find_all('span', class_='a-offscreen'):
                sale_price = soup.find_all(
                    'span', class_='a-offscreen')[0].text.strip()
            else:
                sale_price = '-'
            if soup.find_all('h5', class_='aod-asin-title-text-class'):
                title = soup.find_all(
                    'h5', class_='aod-asin-title-text-class')[0].text.strip()
            else:
                title = '-'
            if soup.find_all('span', id='aod-asin-reviews-count-title'):
                rating = soup.find_all(
                    'span', id='aod-asin-reviews-count-title')[0].text.strip()
            else:
                rating = '-'
            if soup.find_all('i', id='aod-asin-reviews-star'):
                star = soup.find_all(
                    'i', id='aod-asin-reviews-star')[0]['class'][-1][7:]
            else:
                star = '-'
            dict1['Asin'].append(asin)
            dict1['Sale_price'].append(sale_price)
            print(asin,sale_price)
            dict1['title'].append(title)
            dict1['rating'].append(rating)
            dict1['star'].append(star)
        df = pd.DataFrame(dict1)
        csv_path1 = r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\Amazon爬虫\Amazon_PriceOutput_{}_{}.csv'.format(date,country)
        df.to_csv(csv_path1,encoding='gb18030',index_label=None)
    e = time()
    bot_push_text('{}\n总用时：{}s'.format(os.path.basename(__file__),strftime("%H:%M:%S", gmtime(e - s))))