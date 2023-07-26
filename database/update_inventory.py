# -*- coding: gbk -*-
import pandas as pd
from datetime import datetime
import pyodbc
import os



class Config:
    def __init__(self):
        self.server = '47.119.164.123'
        self.database = '39F'
        self.username = 'harryyan'
        self.password = 'Aa12345678'


class DB:
    def __init__(self):
        config = Config()
        server = config.server
        database = config.database
        username = config.username
        password = config.password
        self.cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
            server +
            ';DATABASE=' +
            database +
            ';UID=' +
            username +
            ';PWD=' +
            password)
        self.cursor = self.cnxn.cursor()

    def disconnect(self):
        print('\nSuccessfully disconnected!\n')
        self.cursor.close()

    def get_df(self, query):
        return pd.read_sql(query, self.cnxn)

    def run(self, query):
        self.cursor.execute(query)
        self.cursor.commit()
        print('Successfully run!\n')

    def commit(self):
        self.cursor.commit()


def main(date, dir):
    # main1()

    today = datetime.strftime(datetime.strptime(date, '%Y%m%d'), '%Y-%m-%d')

    df_inv = pd.read_excel(dir, sheet_name='入库商家补货计划表')

    # clean 'Spare Parts'
    df_inv = df_inv[df_inv['类别category'] != 'Spare Parts']

    df_inv = df_inv[df_inv['四字机构'] == '太华天街']

    # 1. selected columns
    cols = ['序号', '国家', '对应账号SKU', '类别category', '销量趋势(前30天对比（前60天到前30天）销量)',
            '第一次断货时间', '第二次断货时间', '要求ETD', '要求验货时间',
            '要求完工时间\n订舱\n调拨单', '要求下单时间', '建议下单数量', '货号', '入库商家',
            '机构', 'CBM', 'FOB出运港', '工厂']

    warehouses_all = ['On the Way', 'On the Way 2\n(Wayfair-Castle Gate仓)', 'On the Way 2\n(Wayfair-Castle Gate仓\n US-Castle Gate仓\n CA - Castle Gate仓\n WAYFAIR-DE\n WAYFAIR-FR\n WAYFAIR-UK)','仓1', '仓2', '仓3',
                  'Castle Gate仓', 'FBA仓', 'FBC仓','FBC仓\n(对于MX，此列为FBM仓【Mercado Libre平台库存】)',
                  '出口易库存仓1\n(对于US 美国，仓1为【美国库存】,\n对于EU 欧洲，仓1为【英国库存】)',
                  '出口易库存仓1\n(对于US 美国，仓1为【美国库存】,\n对于CA 加拿大，仓1为【加拿大库存】)\n对于EU 欧洲，仓1为【英国库存】)',
                  '出口易库存仓2\n(对于EU 欧洲，仓2为【法国库存】)', '出口易库存仓3\n(对于EU 欧洲，仓3为【德国库存】)', 'K2',
                  '天池(FR LVA)', '兼爱(FR LBG)', '说剑(US SNA)', '美东一海通（天下）\n(US ATL)',
                  '美东（天运）\n(US PHL)', '美西一海通（达生）\n(US BUR)', '美西（天道）\n(US LSQ)',
                  '圆融汇通英国(让王)\n(UK MAN)', '圆融汇通（逍遥）\n(CA YTZ)', '德国嘉宏（山木）\n(DE DUS)',
                  '大森林（宗师）\n(FR BVA)', '桑楚\n(US PDK)', '北游\n(US ONO)', '齐物\n(US CCP)',
                  '养生\n(DE NRN)', '人间\n(US GSP)', 'RICHMOND HILL', 'Castle\n(OVERSTOCK)',
                  'CPA\n(OVERSTOCK)', 'WKC\n(OVERSTOCK)','三角洲Delta','Houston',
                  'US-OVERSTOCK-CALIFORNIA\n(OVERSTOCK)', '公冶(FR BOD)',
                  'Groupon仓\n(NL AMS)','说疑\n(CA YMX)','问辩\n(CA YUL)','FR DOL','DE FKB','CL SCL','初见\n(FR LYS)']
    warehouses = []
    for warehouse in list(df_inv):
        if warehouse in warehouses_all:
            warehouses.append(warehouse)
    df_unpivot = df_inv.melt(
        id_vars=cols,
        value_vars=warehouses,
        var_name='warehouses')

    df_wh = df_unpivot[['序号', '国家', '对应账号SKU', '类别category', '机构', 'CBM', 'FOB出运港',
                        '入库商家', '工厂', 'warehouses', 'value']]

    df_wh.columns = ['No', 'Country', 'Item Number', 'Category', 'Operation detail',
                     'CBM', 'FOB-City', 'platform', 'Factory', 'warehouse detail', 'InvBal-Qty']

    df_wh = df_wh[df_wh['InvBal-Qty'].notnull()].reset_index(drop=True)

    # warehouse category

    map_WH = {'On the Way': ['On the Way', 'On the Way 2\n(Wayfair-Castle Gate仓)','On the Way 2\n(Wayfair-Castle Gate仓\n US-Castle Gate仓\n CA - Castle Gate仓\n WAYFAIR-DE\n WAYFAIR-FR\n WAYFAIR-UK)'],
              'WH-Arrival': ['仓1', '仓2', '仓3',
                             'Castle Gate仓', 'FBA仓', 'FBC仓','FBC仓\n(对于MX，此列为FBM仓【Mercado Libre平台库存】)',
                             '出口易库存仓1\n(对于US 美国，仓1为【美国库存】,\n对于EU 欧洲，仓1为【英国库存】)',
                             '出口易库存仓1\n(对于US 美国，仓1为【美国库存】,\n对于CA 加拿大，仓1为【加拿大库存】)\n对于EU 欧洲，仓1为【英国库存】)',
                             '出口易库存仓2\n(对于EU 欧洲，仓2为【法国库存】)', '出口易库存仓3\n(对于EU 欧洲，仓3为【德国库存】)', 'K2',
                                   '天池(FR LVA)', '兼爱(FR LBG)', '说剑(US SNA)', '美东一海通（天下）\n(US ATL)',
                                   '美东（天运）\n(US PHL)', '美西一海通（达生）\n(US BUR)', '美西（天道）\n(US LSQ)',
                                   '圆融汇通英国(让王)\n(UK MAN)', '圆融汇通（逍遥）\n(CA YTZ)', '德国嘉宏（山木）\n(DE DUS)',
                                   '大森林（宗师）\n(FR BVA)', '桑楚\n(US PDK)', '北游\n(US ONO)', '齐物\n(US CCP)',
                                   '养生\n(DE NRN)', '人间\n(US GSP)', 'RICHMOND HILL', 'Castle\n(OVERSTOCK)',
                                   'CPA\n(OVERSTOCK)', 'WKC\n(OVERSTOCK)','三角洲Delta','Houston',
                                   'US-OVERSTOCK-CALIFORNIA\n(OVERSTOCK)', '公冶(FR BOD)',
                                   'Groupon仓\n(NL AMS)','说疑\n(CA YMX)','问辩\n(CA YUL)','FR DOL','DE FKB','CL SCL','初见\n(FR LYS)']}

    maps = []

    for i in map_WH:
        temp = pd.DataFrame(map_WH[i], columns=['warehouse detail'])
        temp['Warehouse'] = i
        maps.append(temp)

    maps_wh = pd.concat(maps).reset_index(drop=True)

    df_wh = df_wh.merge(maps_wh, on='warehouse detail', how='left')
    try:
        ave_sales = df_inv[['序号', 'SKU日均销量']].rename(
            columns={'序号': 'No', 'SKU日均销量': 'Daily Sales'})
    except BaseException:
        ave_sales = df_inv[['序号']].rename(
            columns={'序号': 'No'})
        ave_sales['Daily Sales'] = -1.0
    ave_sales = ave_sales[ave_sales['Daily Sales'].notnull()]

    df = df_wh.merge(ave_sales, on='No', how='left')

    df = df.fillna(0)

    # df['120d sales'] = df['Daily Sales'] * 120

    # df['overstock'] = df.apply(lambda x: x['InvBal-Qty'] - x['120d sales'] if x['InvBal-Qty'] - x['120d sales'] > 0 else 0, axis= 1)

    # df['understock'] = df.apply(lambda x: x['InvBal-Qty'] - x['120d sales'] if x['InvBal-Qty'] - x['120d sales'] < 0 else 0, axis= 1)

    df = df.drop(columns='No')

    # rename df headers
    df.columns = ['Country', 'Item_Number', 'Category', 'operation', 'CBM', 'FOB_City', 'platform', 'Factory',
                  'warehouse_detail', 'InvBal_Qty', 'Warehouse', 'Daily_Sales']

    # record date uploaded
    df['date_upload'] = today

    #####  upload module  #####
    db = DB()

    #####  download headers  #####
    db_header = db.get_df('SELECT TOP(1) * FROM Inventory_Taihua;').columns
    header = '],['.join(db_header)
    values = '?,' * len(db_header)

    #####  delete table  #####
    # db.run('DELETE FROM [Inventory];')

    #####  insert into db  #####
    print('Now insert values')
    # insert to Database
    t0 = datetime.now()
    for index, row in df.iterrows():
        db.cursor.execute(f"INSERT INTO Inventory_Taihua ([{header}]) values({values[:-1]});" ,
                          row['Country'],
                          row['Item_Number'],
                          row['Category'],
                          row['operation'],
                          row['CBM'],
                          row['FOB_City'],
                          row['platform'],
                          row['Factory'],
                          row['warehouse_detail'],
                          row['InvBal_Qty'],
                          row['Warehouse'],
                          row['Daily_Sales'],
                          row['date_upload']
                          )
    db.cursor.commit()

    t1 = datetime.now()
    print('\nTime used: ', t1 - t0)

    db.disconnect()
    pass


def run():
    g = os.walk(r"D:\Shadowbot")

    full_path_list = []
    for path, dir_list, file_list in g:
        for file_name in file_list:
            full_path = os.path.join(path, file_name)
            file_date = file_name[:8]

            full_path_list.append((file_date, full_path))

    for date, dir in full_path_list:
        print(date)
        main(date, dir)


if __name__ == '__main__':
    run()
