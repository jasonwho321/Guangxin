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

    df_inv = pd.read_excel(dir, sheet_name='����̼Ҳ����ƻ���')

    # clean 'Spare Parts'
    df_inv = df_inv[df_inv['���category'] != 'Spare Parts']

    df_inv = df_inv[df_inv['���ֻ���'] == '̫�����']

    # 1. selected columns
    cols = ['���', '����', '��Ӧ�˺�SKU', '���category', '��������(ǰ30��Աȣ�ǰ60�쵽ǰ30�죩����)',
            '��һ�ζϻ�ʱ��', '�ڶ��ζϻ�ʱ��', 'Ҫ��ETD', 'Ҫ�����ʱ��',
            'Ҫ���깤ʱ��\n����\n������', 'Ҫ���µ�ʱ��', '�����µ�����', '����', '����̼�',
            '����', 'CBM', 'FOB���˸�', '����']

    warehouses_all = ['On the Way', 'On the Way 2\n(Wayfair-Castle Gate��)', 'On the Way 2\n(Wayfair-Castle Gate��\n US-Castle Gate��\n CA - Castle Gate��\n WAYFAIR-DE\n WAYFAIR-FR\n WAYFAIR-UK)','��1', '��2', '��3',
                  'Castle Gate��', 'FBA��', 'FBC��','FBC��\n(����MX������ΪFBM�֡�Mercado Libreƽ̨��桿)',
                  '�����׿���1\n(����US ��������1Ϊ��������桿,\n����EU ŷ�ޣ���1Ϊ��Ӣ����桿)',
                  '�����׿���1\n(����US ��������1Ϊ��������桿,\n����CA ���ô󣬲�1Ϊ�����ô��桿)\n����EU ŷ�ޣ���1Ϊ��Ӣ����桿)',
                  '�����׿���2\n(����EU ŷ�ޣ���2Ϊ��������桿)', '�����׿���3\n(����EU ŷ�ޣ���3Ϊ���¹���桿)', 'K2',
                  '���(FR LVA)', '�氮(FR LBG)', '˵��(US SNA)', '����һ��ͨ�����£�\n(US ATL)',
                  '���������ˣ�\n(US PHL)', '����һ��ͨ��������\n(US BUR)', '�����������\n(US LSQ)',
                  'Բ�ڻ�ͨӢ��(����)\n(UK MAN)', 'Բ�ڻ�ͨ����ң��\n(CA YTZ)', '�¹��κ꣨ɽľ��\n(DE DUS)',
                  '��ɭ�֣���ʦ��\n(FR BVA)', 'ɣ��\n(US PDK)', '����\n(US ONO)', '����\n(US CCP)',
                  '����\n(DE NRN)', '�˼�\n(US GSP)', 'RICHMOND HILL', 'Castle\n(OVERSTOCK)',
                  'CPA\n(OVERSTOCK)', 'WKC\n(OVERSTOCK)','������Delta','Houston',
                  'US-OVERSTOCK-CALIFORNIA\n(OVERSTOCK)', '��ұ(FR BOD)',
                  'Groupon��\n(NL AMS)','˵��\n(CA YMX)','�ʱ�\n(CA YUL)','FR DOL','DE FKB','CL SCL','����\n(FR LYS)']
    warehouses = []
    for warehouse in list(df_inv):
        if warehouse in warehouses_all:
            warehouses.append(warehouse)
    df_unpivot = df_inv.melt(
        id_vars=cols,
        value_vars=warehouses,
        var_name='warehouses')

    df_wh = df_unpivot[['���', '����', '��Ӧ�˺�SKU', '���category', '����', 'CBM', 'FOB���˸�',
                        '����̼�', '����', 'warehouses', 'value']]

    df_wh.columns = ['No', 'Country', 'Item Number', 'Category', 'Operation detail',
                     'CBM', 'FOB-City', 'platform', 'Factory', 'warehouse detail', 'InvBal-Qty']

    df_wh = df_wh[df_wh['InvBal-Qty'].notnull()].reset_index(drop=True)

    # warehouse category

    map_WH = {'On the Way': ['On the Way', 'On the Way 2\n(Wayfair-Castle Gate��)','On the Way 2\n(Wayfair-Castle Gate��\n US-Castle Gate��\n CA - Castle Gate��\n WAYFAIR-DE\n WAYFAIR-FR\n WAYFAIR-UK)'],
              'WH-Arrival': ['��1', '��2', '��3',
                             'Castle Gate��', 'FBA��', 'FBC��','FBC��\n(����MX������ΪFBM�֡�Mercado Libreƽ̨��桿)',
                             '�����׿���1\n(����US ��������1Ϊ��������桿,\n����EU ŷ�ޣ���1Ϊ��Ӣ����桿)',
                             '�����׿���1\n(����US ��������1Ϊ��������桿,\n����CA ���ô󣬲�1Ϊ�����ô��桿)\n����EU ŷ�ޣ���1Ϊ��Ӣ����桿)',
                             '�����׿���2\n(����EU ŷ�ޣ���2Ϊ��������桿)', '�����׿���3\n(����EU ŷ�ޣ���3Ϊ���¹���桿)', 'K2',
                                   '���(FR LVA)', '�氮(FR LBG)', '˵��(US SNA)', '����һ��ͨ�����£�\n(US ATL)',
                                   '���������ˣ�\n(US PHL)', '����һ��ͨ��������\n(US BUR)', '�����������\n(US LSQ)',
                                   'Բ�ڻ�ͨӢ��(����)\n(UK MAN)', 'Բ�ڻ�ͨ����ң��\n(CA YTZ)', '�¹��κ꣨ɽľ��\n(DE DUS)',
                                   '��ɭ�֣���ʦ��\n(FR BVA)', 'ɣ��\n(US PDK)', '����\n(US ONO)', '����\n(US CCP)',
                                   '����\n(DE NRN)', '�˼�\n(US GSP)', 'RICHMOND HILL', 'Castle\n(OVERSTOCK)',
                                   'CPA\n(OVERSTOCK)', 'WKC\n(OVERSTOCK)','������Delta','Houston',
                                   'US-OVERSTOCK-CALIFORNIA\n(OVERSTOCK)', '��ұ(FR BOD)',
                                   'Groupon��\n(NL AMS)','˵��\n(CA YMX)','�ʱ�\n(CA YUL)','FR DOL','DE FKB','CL SCL','����\n(FR LYS)']}

    maps = []

    for i in map_WH:
        temp = pd.DataFrame(map_WH[i], columns=['warehouse detail'])
        temp['Warehouse'] = i
        maps.append(temp)

    maps_wh = pd.concat(maps).reset_index(drop=True)

    df_wh = df_wh.merge(maps_wh, on='warehouse detail', how='left')
    try:
        ave_sales = df_inv[['���', 'SKU�վ�����']].rename(
            columns={'���': 'No', 'SKU�վ�����': 'Daily Sales'})
    except BaseException:
        ave_sales = df_inv[['���']].rename(
            columns={'���': 'No'})
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
