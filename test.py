from Wayfair_Scrapy import *
from Wayfair_Monitor import *
csv_path1 = r'C:\Users\Administrator\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\Monitior\Wayfair_PriceOutput_20221125220000_US.csv'
df = mapping_sku(
    csv_path1,
    r'C:\Users\Administrator\Nutstore\1\「晓望集群」\S数据分析\Wayfair爬虫\SKU_Mapping_{}.csv'.format('US'), 'full_sku',
    'partner_number')
monitor_notice(df,'US')