import pandas as pd
import xlwings as xw
import math


# 定义一个名为Get_Inven的函数
def Get_Inven(
        file_dir=f'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/尾程运费/InventoryCode2023-09-18.xlsx'):
    # 从指定的CSV文件路径中读取数据，存入DataFrame df中
    df = pd.read_excel(file_dir)

    # 从df中选取'Goods Name','Packing Size','Gross Weight'三列，并创建一个新的DataFrame df1
    df1 = pd.DataFrame(df, columns=['Goods Name', 'Packing Size', 'Gross Weight'])

    # 使用str.split方法，将'Packing Size'列的字符串按照'*'分隔，并扩展为多列，存入新的DataFrame df_2中
    df_2 = df1['Packing Size'].str.split('*', expand=True)

    # 使用pd.concat方法，沿着列的方向（axis=1）将df1和df_2合并为一个新的DataFrame
    df1 = pd.concat([df1, df_2], axis=1)

    # 重命名df1中的新列为'Long','Width','Height'
    df1 = df1.rename(columns={0: 'Long', 1: 'Width', 2: 'Height'})

    # 最后，重新选取所需的列，并创建一个新的DataFrame df1
    df1 = pd.DataFrame(df1, columns=['Goods Name', 'Packing Size', 'Gross Weight', 'Long', 'Width', 'Height'])

    # 返回处理后的DataFrame df1
    return df1


def Get_Inven1(
        file_dir=f'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/尾程运费/InventoryCode2023-09-18.xlsx'):
    df = pd.read_csv(file_dir)
    df1 = pd.DataFrame(df, columns=['Goods Name', 'Packing Size', 'Gross Weight', 'Long', 'Width', 'Height'])
    return df, df1


def Get_Fed1_price(
        file_dir=f'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/尾程运费/220527 Toronto&Northyork&Richmond Hill 物流报价-FEDEX.xlsx',
        sheet=0, range='A3:I133'):
    app = xw.App(visible=True, add_book=False)
    book = app.books.open(file_dir)
    sheet = book.sheets[sheet]
    value = sheet.range(range).options(pd.DataFrame, header=1, index=0).value
    book.close()
    app.quit()
    return value


def Get_UPS_Price():
    app = xw.App(visible=True, add_book=False)
    book = app.books.open(
        r'E:\OneDrive\露露\220128 ups 报价--rancho ontario atlanta us lax us ccp us aus us jfk us alt .xlsx')
    sheet = book.sheets[0]
    value = sheet.range('A2:I152').options(pd.DataFrame, header=1, index=0).value
    book.close()
    app.quit()
    return value


def Coun_UPS_Fre(df1, value):
    fre_list = []
    for i in range(len(df1)):
        print(i)
        dic = df1.iloc[i].to_dict()
        freight = 0.00
        L = float(dic['Long'])
        W = float(dic['Width'])
        H = float(dic['Height'])
        GW = float(dic['Gross Weight'])
        LWH_list = [L, W, H]
        LWH_list.sort()
        try:
            if GW > 67.5 or LWH_list[-1] > 274 or (L + 2 * (W + H)) > 419:
                freight = 9999.00
            else:
                GWLBS = GW / 0.45
                VOL = (L * 0.3937 * W * 0.3937 * H * 0.3937) / 139
                WG_list = [GWLBS, VOL]
                WG_list.sort()
                FinalGW = WG_list[-1]

                FinalGW = math.ceil(FinalGW)

                if GWLBS > 50.0:
                    Over_Wet = 20.4
                else:
                    Over_Wet = 0.0

                if LWH_list[-1] > 121.0 or LWH_list[-2] > 76.0 or 330.0 >= (L + 2 * (W + H)) > 266.0:
                    Over_lon = 10.7
                else:
                    Over_lon = 0.0

                if 419.0 >= (L + 2 * (W + H)) > 330.0:
                    Over_Vol = 110.0
                    if Over_lon != 0.0 and FinalGW < 90:
                        FinalGW = 90
                else:
                    Over_Vol = 0.0

                house_fee = 3.8
                base_Fre = value.loc[value['≤磅/lbs'] == float(FinalGW), 'Zone 8']

                base_Fre = float(base_Fre.iloc[0])

                add_up = base_Fre + Over_Vol + Over_Wet + Over_lon + house_fee
                Fuel_add = add_up * 0.13
                freight = freight + Fuel_add + add_up
        except:
            freight = "NA"
        fre_list.append(freight)

    df1['UPS_Freight'] = fre_list

    df1.to_csv(r'E:\OneDrive\露露\邮费.csv')


def Coun_Fedex_Fre(df1, value):
    fre_list = []
    for i in range(len(df1)):
        print(i)
        dic = df1.iloc[i].to_dict()
        freight = 0.00
        L = float(dic['Long'])
        W = float(dic['Width'])
        H = float(dic['Height'])
        GW = float(dic['Gross Weight'])
        LWH_list = [L, W, H]
        LWH_list.sort()
        try:
            if GW > 67.5 or LWH_list[-1] > 274 or (LWH_list[-1] + 2 * (LWH_list[-2] + LWH_list[-3])) > 419:
                freight = 9999.00
            else:
                GWLBS = GW / 0.45
                VOL = (L * 0.3937 * W * 0.3937 * H * 0.3937) / 250
                WG_list = [GWLBS, VOL]
                WG_list.sort()
                FinalGW = WG_list[-1]

                FinalGW = math.ceil(FinalGW)

                Over_Wet = 0.0

                if LWH_list[-1] > 243.84 or (LWH_list[-1] + 2 * (LWH_list[-2] + LWH_list[-3])) > 330.0:
                    Over_Vol = 48.4
                    if FinalGW < 90:
                        FinalGW = 90
                else:
                    Over_Vol = 0.0

                AHS = 0.0
                if (LWH_list[-1] + 2 * (LWH_list[-2] + LWH_list[-3])) < 330.0:
                    if GWLBS > 50:
                        AHS += 11.55
                    if LWH_list[-1] > 122 or LWH_list[-2] > 76.2 or (L + 2 * (W + H)) * 0.3937 > 267:
                        AHS += 8.8

                house_fee = 4.95
                base_Fre = value.loc[value['≤磅/lbs'] == float(FinalGW), 'Zone 8']

                base_Fre = float(base_Fre.iloc[0])

                add_up = base_Fre + Over_Vol + Over_Wet + +house_fee
                freight = freight + +add_up
        except:
            freight = "NA"
        fre_list.append(freight)

    df1['Fedex_Freight'] = fre_list

    df1.to_csv(r'E:\OneDrive\露露\邮费.csv')


def Coun_Fed1_Fre(df1, value, df):
    fre_list = []
    for i in range(len(df1)):
        print(i)
        dic = df1.iloc[i].to_dict()
        freight = 0.00
        L = float(dic['Long'])
        W = float(dic['Width'])
        H = float(dic['Height'])
        GW = float(dic['Gross Weight'])
        LWH_list = [L, W, H]
        LWH_list.sort()
        try:
            if GW > 67.5 or LWH_list[-1] > 274 or (L + 2 * (W + H)) > 419:
                freight = 9999.00
            else:
                GWLBS = GW / 0.45
                VOL = (L * 0.3937 * W * 0.3937 * H * 0.3937) / 194
                WG_list = [GWLBS, VOL]
                WG_list.sort()
                FinalGW = WG_list[-1]

                FinalGW = math.ceil(FinalGW)

                if GWLBS > 50.0:
                    Over_Wet = 17.05 + 5.39
                else:
                    Over_Wet = 0.0

                if LWH_list[-1] > 121.0 or LWH_list[-2] > 76.0 or 330.0 >= (L + 2 * (W + H)) > 266.0:
                    Over_lon = 10.45 + 5.39
                else:
                    Over_lon = 0.0

                if 419.0 >= (L + 2 * (W + H)) > 330.0 or LWH_list[-1] > 243.0:
                    Over_Vol = 71.5 + 29.15
                    if Over_lon != 0.0 and FinalGW < 90:
                        FinalGW = 90
                else:
                    Over_Vol = 0.0

                house_fee = 3.85
                base_Fre = value.loc[value['Lbs.'] == float(FinalGW), 'Zone 8']

                base_Fre = float(base_Fre.iloc[0])

                add_up = base_Fre + Over_Vol + Over_Wet + Over_lon + house_fee
                Fuel_add = add_up * 0.13
                freight = freight + Fuel_add + add_up
        except:
            freight = "NA"
        fre_list.append(freight)

    df['Fed1_Freight'] = fre_list

    df.to_csv(r'E:\OneDrive\露露\邮费.csv')


def Coun_Fed_Ground(df1, value, value1):
    # 创建一个新的DataFrame，用于保存最终结果
    result_df = pd.DataFrame(columns=['Goods Name',
                                      'Zone 0',
                                      'Zone 1',
                                      'Zone 2',
                                      'Zone 3',
                                      'Zone 4',
                                      'Zone 5',
                                      'Zone 6',
                                      'Zone 7',
                                      'Zone 8',
                                      'Zone 9',
                                      'Zone 10',
                                      'Zone 11',
                                      'Zone 12',
                                      'Zone 13',
                                      'Zone 14',
                                      'Zone 15',
                                      'Zone 16',
                                      '状态'])

    for i in range(len(df1)):
        print(i)
        dic = df1.iloc[i].to_dict()
        zone_fees = {}
        status = ""

        try:

            L = float(dic['Long'].split(',')[0])  # 使用第一个数值
            W = float(dic['Width'].split(',')[0])  # 使用第一个数值
            H = float(dic['Height'].split(',')[0])  # 使用第一个数值
            GW = float(dic['Gross Weight'].split(',')[0])  # 使用第一个数值

            LWH_list = [L, W, H]
            LWH_list.sort()

            GWLBS = GW / 0.45
            VOL = (L * W * H) / 5000

            WG_list = [GWLBS, VOL]
            WG_list.sort()

            FinalGW = WG_list[-1]
            FinalGW = math.ceil(FinalGW)
            # 判断是否超重
            if GW > 68:
                status = "超重"
            # 判断是否超长
            elif LWH_list[-1] > 274 or (L + 2 * (W + H)) > 419:
                status = "超规"
            if GW > 31.0:
                Over_Wet = 26.67
            else:
                Over_Wet = 0.0

            if LWH_list[-1] > 121.0 or LWH_list[-2] > 76.0 or 330.0 >= (L + 2 * (W + H)) > 266.0:
                Over_lon = 23.33
            else:
                Over_lon = 0.0

            if 419.0 >= (L + 2 * (W + H)) > 330.0 or LWH_list[-1] > 243.0:
                Over_Vol = 105.56
                if Over_lon != 0.0 and FinalGW < 90:
                    FinalGW = 90
            else:
                Over_Vol = 0.0

            house_fee = 4.72
            if float(FinalGW) > 20.0:
                for zone in range(0, 7):  # Zone 0 to Zone 6
                    base_Fre = float(value.loc[value['LBS'] == float(FinalGW), f'Zone {zone}'].iloc[0])
                    add_up = base_Fre + Over_Vol + Over_Wet + Over_lon + house_fee
                    Fuel_add = add_up * 0.35
                    zone_fees[f'Zone {zone}'] = round(add_up + Fuel_add, 2)
            else:

                for zone in range(1, 16):  # Zone 1 to Zone 16
                    base_Fre = float(value1.loc[value1['lbs.'] == str(int(FinalGW)), f'ZONE{zone}'].iloc[0])
                    add_up = base_Fre + Over_Vol + Over_Wet + Over_lon + house_fee
                    Fuel_add = add_up * 0.35
                    zone_fees[f'Zone {zone}'] = round(add_up + Fuel_add, 2)

        except Exception as e:
            print(e)
            status = "箱规不全"

        goods_name = dic.get('Goods Name', "Unknown")  # 假设原始df1中有一个叫做'Goods Name'的列
        result_df.loc[len(result_df)] = [goods_name] + [zone_fees.get(f'Zone {zone}', "") for zone in range(0, 17)] + [
            status]

    result_df.to_csv(
        r'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/尾程运费/邮费.csv',
        encoding='gb18030')


def main_ups():
    df1 = Get_Inven()
    value = Get_UPS_Price()
    Coun_UPS_Fre(df1, value)


def main_Fed1():
    df, df1 = Get_Inven1()
    value = Get_Fed1_price()
    Coun_Fed1_Fre(df1, value, df)


def main_Fed_CA_ground():
    df1 = Get_Inven()
    value = Get_Fed1_price()
    value1 = Get_Fed1_price(sheet=1, range='A3:Q23')
    Coun_Fed_Ground(df1, value, value1)


def Coun_all(df1, value_dfs, additional_fees):
    # 创建一个新的DataFrame，用于保存最终结果
    result_df = pd.DataFrame(columns=['Goods Name',
                                      'Zone 2',
                                      'Zone 3',
                                      'Zone 4',
                                      'Zone 5',
                                      'Zone 6',
                                      'Zone 7',
                                      'Zone 8',
                                      '状态'])

    # 根据zd的key循环所有运费表
    for zd_key in value_dfs.keys():
        current_fees = additional_fees[zd_key]
        for i in range(len(df1)):
            dic = df1.iloc[i].to_dict()

            min_zone_fees = {f'Zone {zone}': float('inf') for zone in range(2, 9)}
            status = ""

            L = float(dic['Long'].split(',')[0])  # 使用第一个数值
            W = float(dic['Width'].split(',')[0])  # 使用第一个数值
            H = float(dic['Height'].split(',')[0])  # 使用第一个数值
            GW = float(dic['Gross Weight'].split(',')[0])  # 使用第一个数值

            LWH_list = [L, W, H]
            LWH_list.sort()

            GWLBS = GW / 0.45
            VOL = (L * W * H) / 5000

            WG_list = [GWLBS, VOL]
            WG_list.sort()

            FinalGW = WG_list[-1]
            FinalGW = math.ceil(FinalGW)
            for zone in range(2, 9):
                zd = value_dfs[zd_key]
                # 根据相应的zone选择适当的附加费
                if zone == 2:
                    fees = current_fees['Zone 2']
                elif 3 <= zone <= 4:
                    fees = current_fees['Zone 3-4']
                elif 5 <= zone <= 6:
                    fees = current_fees['Zone 5-6']
                elif 7 <= zone <= 8:
                    fees = current_fees['Zone 7-8']

                # 确保你获得附加费
                if fees:
                    Over_lon = fees['超长附加费']
                    Over_Wet = fees['超重附加费']
                    house_fee = fees['住宅派送费']
                    Over_Vol = fees['超大附加费']
                # 判断是否超重
                if GW > 68:
                    status = "超重"
                # 判断是否超长
                elif LWH_list[-1] > 274 or (L + 2 * (W + H)) > 419:
                    status = "超规"

                if GW > 22.5:
                    Over_Wet = Over_Wet
                else:
                    Over_Wet = 0.0

                if LWH_list[-1] > 121.0 or LWH_list[-2] > 76.0 or 330.0 >= (L + 2 * (W + H)) > 266.0:
                    Over_lon = Over_lon
                else:
                    Over_lon = 0.0

                if 419.0 >= (L + 2 * (W + H)) > 330.0 or LWH_list[-1] > 243.0:
                    Over_Vol = Over_Vol
                    if Over_lon != 0.0 and FinalGW < 90:
                        FinalGW = 90
                else:
                    Over_Vol = 0.0
                temp_series = zd.loc[zd['LBS'] == float(FinalGW), f'Zone {zone}']

                if not temp_series.empty:
                    base_Fre = float(temp_series.iloc[0])
                else:
                    print(f"No rates found for LBS = {FinalGW} in Zone {zone}")
                    continue  # skip to next iteration in for zone in range(2, 9) loop

                add_up = base_Fre + Over_Vol + Over_Wet + Over_lon + house_fee
                Fuel_add = add_up * current_fees['燃油附加费']
                zone_fee = round(add_up + Fuel_add, 2)

                # 找到这个zone的最小zone_fee
                if zone_fee < min_zone_fees[f'Zone {zone}']:
                    min_zone_fees[f'Zone {zone}'] = zone_fee

            goods_name = dic.get('Goods Name', 'Unknown')
            result_df.loc[len(result_df)] = [goods_name] + [min_zone_fees[f'Zone {zone}'] for zone in
                                                            range(2, 9)] + [status]
    # 分组并获取最小值
    result_grouped = result_df.groupby('Goods Name').min()
    result_grouped.reset_index(inplace=True)
    result_grouped.to_csv(
        r'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/尾程运费/邮费_0206.csv',
        encoding='gb18030')

def read_excel_sheets(path):
    # 这个函数返回一个字典，其中包含Excel文件的所有工作表数据
    with pd.ExcelFile(path) as xls:
        data = pd.read_excel(xls, sheet_name=None)
    return data


if __name__ == '__main__':
    sku_df = Get_Inven(
        '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/尾程运费/02 SKUs清单.xlsx')
    dir = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/尾程运费/协议报价汇总.xlsx'
    # 所有Excel文件的工作表数据
    value_dfs = read_excel_sheets(dir)
    additional_fees = {
        'FEDEX_KPL_W': {
            'Zone 2': {
                '超长附加费': 7.1,
                '超重附加费': 5.8,
                '住宅派送费': 3.9,
                '超大附加费': 70.6},
            'Zone 3-4': {
                '超长附加费': 7.1,
                '超重附加费': 5.8,
                '住宅派送费': 3.9,
                '超大附加费': 70.6},
            'Zone 5-6': {
                '超长附加费': 7.1,
                '超重附加费': 5.8,
                '住宅派送费': 3.9,
                '超大附加费': 70.6},
            'Zone 7-8': {
                '超长附加费': 7.1,
                '超重附加费': 5.8,
                '住宅派送费': 3.9,
                '超大附加费': 70.6},
            '燃油附加费':0.13
        },
        'Fedex-RLSON': {
            'Zone 2': {
                '超长附加费': 7.2,
                '超重附加费': 11.28,
                '住宅派送费': 3.06,
                '超大附加费': 35.56,
            },
            'Zone 3-4': {
                '超长附加费': 7.98,
                '超重附加费': 12.26,
                '住宅派送费': 3.06,
                '超大附加费': 37.78,
            },
            'Zone 5-6': {
                '超长附加费': 8.76,
                '超重附加费': 13.03,
                '住宅派送费': 3.06,
                '超大附加费': 42.22,
            },
            'Zone 7-8': {
                '超长附加费': 9.72,
                '超重附加费': 14,
                '住宅派送费': 3.06,
                '超大附加费': 44.44,
            },
            '燃油附加费':0.1625
        },
        'Fedex-RSSON': {
            'Zone 2': {
                '超长附加费': 7.2,
                '超重附加费': 11.28,
                '超大附加费': 35.56,
                '住宅派送费': 3.06,
            },
            'Zone 3-4': {
                '超长附加费': 7.98,
                '超重附加费': 12.26,
                '超大附加费': 37.78,
                '住宅派送费': 3.06,
            },
            'Zone 5-6': {
                '超长附加费': 8.76,
                '超重附加费': 13.03,
                '超大附加费': 42.22,
                '住宅派送费': 3.06,
            },
            'Zone 7-8': {
                '超长附加费': 9.72,
                '超重附加费': 14,
                '超大附加费': 44.44,
                '住宅派送费': 3.06,
            },
            '燃油附加费':0.1625
        },
        'FedEx-GUD': {
            'Zone 2': {
                '超长附加费': 21,
                '超重附加费': 24,
                '住宅派送费': 4.25,
                '超大附加费': 95,
            },
            'Zone 3-4': {
                '超长附加费': 21,
                '超重附加费': 24,
                '住宅派送费': 4.25,
                '超大附加费': 95,
            },
            'Zone 5-6': {
                '超长附加费': 21,
                '超重附加费': 24,
                '住宅派送费': 4.25,
                '超大附加费': 95,
            },
            'Zone 7-8': {
                '超长附加费': 21,
                '超重附加费': 24,
                '住宅派送费': 4.25,
                '超大附加费': 95,
            },
            '燃油附加费':0.13
        },
        'UPS-Mofangk2-Ground': {
            'Zone 2': {
                '超长附加费': 8.22,
                '超重附加费': 12.89,
                '住宅派送费': 3.21,
                '超大附加费': 71.11,
            },
            'Zone 3-4': {
                '超长附加费': 9.11,
                '超重附加费': 14,
                '住宅派送费': 3.21,
                '超大附加费': 75.56
            },
            'Zone 5-6': {
                '超长附加费': 10.44,
                '超重附加费': 15.33,
                '住宅派送费': 3.21,
                '超大附加费': 86.67,
            },
            'Zone 7-8': {
                '超长附加费': 10.44,
                '超重附加费': 15.33,
                '住宅派送费': 3.21,
                '超大附加费': 86.67,
            },
            '燃油附加费':0.1625
        },
        'Fedex-Mofangk2-Ground': {
            'Zone 2': {
                '超长附加费': 8.22,
                '超重附加费': 12.89,
                '住宅派送费': 3.21,
                '超大附加费': 71.11,
            },
            'Zone 3-4': {
                '超长附加费': 9.11,
                '超重附加费': 14,
                '住宅派送费': 3.21,
                '超大附加费': 75.56
            },
            'Zone 5-6': {
                '超长附加费': 10.44,
                '超重附加费': 15.33,
                '住宅派送费': 3.21,
                '超大附加费': 86.67,
            },
            'Zone 7-8': {
                '超长附加费': 10.44,
                '超重附加费': 15.33,
                '超大附加费': 86.67,
                '住宅派送费': 3.21,
            },
            '燃油附加费':0.1625
        },
        'QY': {
            'Zone 2': {
                '超重附加费': 2.6,
                '超长附加费': 4.09,
                '住宅派送费': 3.39,
                '超大附加费': 47.64,
            },
            'Zone 3-4': {
                '超重附加费': 2.91,
                '超长附加费': 4.46,
                '住宅派送费': 3.39,
                '超大附加费': 52.11
            },
            'Zone 5-6': {
                '超重附加费': 3.2,
                '超长附加费': 4.74,
                '住宅派送费': 3.39,
                '超大附加费': 56.58,
            },
            'Zone 7-8': {
                '超重附加费': 3.56,
                '超长附加费': 5.16,
                '住宅派送费': 3.39,
                '超大附加费': 61.04,
            },
            '燃油附加费':0.1625
        }}
    Coun_all(sku_df,value_dfs,additional_fees)

