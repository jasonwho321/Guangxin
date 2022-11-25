from Wayfair_Scrapy import *
from datetime import timedelta
from time import sleep
csv_path_CA = r'C:\Users\Administrator\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Monitior\wayfair_Monitor_CA.csv'
csv_path_US = r'C:\Users\Administrator\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Monitior\wayfair_Monitor_US.csv'


def monitor_notice(df):
    data = df[(df['option'] == '-')]
    invalid_sku = data['sku'].values.tolist()
    data = df[(df['stock'] == 'out_of_stock')]
    out_of_stock_sku = data['partner_number'].values.tolist()

    bot_push_text('已失效SKU：\n{}'.format('\n'.join(invalid_sku)))

    bot_push_text('显示缺货Part Number：\n{}'.format('\n'.join(out_of_stock_sku)))


if __name__ == '__main__':
    while True:
        now = datetime.now()
        str_sched_Time = str(now - timedelta(hours=-1))[:13] + ':00:00'
        sched_Time = datetime.strptime(str_sched_Time, '%Y-%m-%d %H:%M:%S')
        delay = (sched_Time - datetime.now()).total_seconds()
        sleep(delay)

        country_list = ["US", "CA"]
        s = time()
        for country in country_list:
            proxy = '221.131.141.243:9091'
            csv_path = csv_path_US if country == "US" else csv_path_CA
            data = read_src(csv_path)
            lenth = len(data)
            date = datetime.now().strftime("%Y%m%d%H%M%S")

            manager = Manager()
            lock = manager.Lock()
            dict1 = manager.dict()
            dict1['country'] = country
            table1 = manager.list()
            pool_num = cpu_count()
            cookie_pool = manager.list()

            for i in range(pool_num):
                cookie_pool.append(get_cookies(country))

            pbar = tqdm(total=lenth)
            update = lambda *args: pbar.update(1)
            workers = Pool(pool_num)
            for sku in data:
                workers.apply_async(
                    process, (sku, table1, dict1, lock, cookie_pool,), callback=update)
            workers.close()
            workers.join()
            table1.insert(0, ['SKU', 'option', 'full_list', 'title', 'stock','price','rate','reviews'])
            csv_path1 = r'C:\Users\Administrator\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Monitior\Wayfair_PriceOutput_' + \
                date + '_' + country + '.csv'
            with open(csv_path1, 'w', encoding='utf_8_sig', newline='') as f:
                writer = csv.writer(f, dialect='excel')
                writer.writerows(table1)
            df = mapping_sku(
                csv_path1,
                r'C:\Users\Administrator\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_Mapping_{}.csv'.format(country), 'full_sku', 'partner_number')
            monitor_notice(df)
        e = time()
        bot_push_text(
            '{}\n总用时：{}s'.format(
                os.path.basename(__file__),
                strftime(
                    "%H:%M:%S",
                    gmtime(
                        e - s))))
