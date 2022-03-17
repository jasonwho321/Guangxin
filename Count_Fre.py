import pandas as pd
import xlwings as xw
import math

def Get_Inven():
    df = pd.read_csv(r'E:\OneDrive\露露\22-03-15-Inventorys (2)(1).csv')
    df1 = pd.DataFrame(df,columns=['Goods Name','Packing Size','Gross Weight'])
    df_2 = df1['Packing Size'].str.split('*',expand=True)
    df1 = pd.concat([df1,df_2],axis=1)
    df1 = df1.rename(columns={0:'Long',1:'Width',2:'Height'})
    df1 = pd.DataFrame(df1,columns=['Goods Name','Packing Size','Gross Weight','Long','Width','Height'])
    return df1

def Get_Inven1():
    df = pd.read_csv(r'E:\OneDrive\露露\邮费.csv')
    df1 = pd.DataFrame(df,columns=['Goods Name','Packing Size','Gross Weight','Long','Width','Height'])
    return df,df1

def Get_Fed1_price():
    app = xw.App(visible=True,add_book=False)
    book = app.books.open(r'E:\OneDrive\露露\物流对外报价\20210118  lsq+phl 对外报价-Noya提供.xlsx')
    sheet = book.sheets[0]
    value = sheet.range('B5:J155').options(pd.DataFrame,header=1,index=0).value
    book.close()
    app.quit()
    return value

def Get_UPS_Price():
    app = xw.App(visible=True,add_book=False)
    book = app.books.open(r'E:\OneDrive\露露\220128 ups 报价--rancho ontario atlanta us lax us ccp us aus us jfk us alt .xlsx')
    sheet = book.sheets[0]
    value = sheet.range('A2:I152').options(pd.DataFrame,header=1,index=0).value
    book.close()
    app.quit()
    return value

def Coun_UPS_Fre(df1,value):
    fre_list = []
    for i in range(len(df1)):
        print(i)
        dic = df1.iloc[i].to_dict()
        freight = 0.00
        L = float(dic['Long'])
        W = float(dic['Width'])
        H = float(dic['Height'])
        GW = float(dic['Gross Weight'])
        LWH_list = [L,W,H]
        LWH_list.sort()
        try:
            if GW > 67.5 or LWH_list[-1] > 274 or (L+2*(W+H))>419:
                freight = 9999.00
            else:
                GWLBS = GW/0.45
                VOL = (L*0.3937*W*0.3937*H*0.3937)/139
                WG_list = [GWLBS,VOL]
                WG_list.sort()
                FinalGW = WG_list[-1]

                FinalGW = math.ceil(FinalGW)

                if GWLBS > 50.0:
                    Over_Wet = 20.4
                else:
                    Over_Wet = 0.0

                if LWH_list[-1] > 121.0 or LWH_list[-2] > 76.0 or 330.0>=(L+2*(W+H))>266.0:
                    Over_lon = 10.7
                else:
                    Over_lon = 0.0

                if 419.0>=(L+2*(W+H))>330.0:
                    Over_Vol = 110.0
                    if Over_lon != 0.0 and FinalGW < 90:
                        FinalGW = 90
                else:
                    Over_Vol = 0.0

                house_fee = 3.8
                base_Fre = value.loc[value['≤磅/lbs']==float(FinalGW),'Zone 8']

                base_Fre = float(base_Fre.iloc[0])

                add_up = base_Fre+Over_Vol+Over_Wet+Over_lon+house_fee
                Fuel_add = add_up*0.13
                freight = freight+Fuel_add+add_up
        except:
            freight = "NA"
        fre_list.append(freight)

    df1['UPS_Freight'] = fre_list

    df1.to_csv(r'E:\OneDrive\露露\邮费.csv')

def Coun_Fedex_Fre(df1,value):
    fre_list = []
    for i in range(len(df1)):
        print(i)
        dic = df1.iloc[i].to_dict()
        freight = 0.00
        L = float(dic['Long'])
        W = float(dic['Width'])
        H = float(dic['Height'])
        GW = float(dic['Gross Weight'])
        LWH_list = [L,W,H]
        LWH_list.sort()
        try:
            if GW > 67.5 or LWH_list[-1] > 274 or (LWH_list[-1]+2*(LWH_list[-2]+LWH_list[-3]))>419:
                freight = 9999.00
            else:
                GWLBS = GW/0.45
                VOL = (L*0.3937*W*0.3937*H*0.3937)/250
                WG_list = [GWLBS,VOL]
                WG_list.sort()
                FinalGW = WG_list[-1]

                FinalGW = math.ceil(FinalGW)

                Over_Wet = 0.0

                if LWH_list[-1] > 243.84 or (LWH_list[-1]+2*(LWH_list[-2]+LWH_list[-3]))>330.0:
                    Over_Vol = 48.4
                    if FinalGW < 90:
                        FinalGW = 90
                else:
                    Over_Vol = 0.0

                AHS = 0.0
                if (LWH_list[-1] + 2 * (LWH_list[-2] + LWH_list[-3])) < 330.0:
                    if GWLBS > 50:
                        AHS+=11.55
                    if LWH_list[-1] > 122 or LWH_list[-2] > 76.2 or (L+2*(W+H))*0.3937>267:
                        AHS += 8.8


                house_fee = 4.95
                base_Fre = value.loc[value['≤磅/lbs']==float(FinalGW),'Zone 8']

                base_Fre = float(base_Fre.iloc[0])

                add_up = base_Fre+Over_Vol+Over_Wet++house_fee
                freight = freight++add_up
        except:
            freight = "NA"
        fre_list.append(freight)

    df1['Fedex_Freight'] = fre_list

    df1.to_csv(r'E:\OneDrive\露露\邮费.csv')

def Coun_Fed1_Fre(df1,value,df):
    fre_list = []
    for i in range(len(df1)):
        print(i)
        dic = df1.iloc[i].to_dict()
        freight = 0.00
        L = float(dic['Long'])
        W = float(dic['Width'])
        H = float(dic['Height'])
        GW = float(dic['Gross Weight'])
        LWH_list = [L,W,H]
        LWH_list.sort()
        try:
            if GW > 67.5 or LWH_list[-1] > 274 or (L+2*(W+H))>419:
                freight = 9999.00
            else:
                GWLBS = GW/0.45
                VOL = (L*0.3937*W*0.3937*H*0.3937)/194
                WG_list = [GWLBS,VOL]
                WG_list.sort()
                FinalGW = WG_list[-1]

                FinalGW = math.ceil(FinalGW)

                if GWLBS > 50.0:
                    Over_Wet = 17.05+5.39
                else:
                    Over_Wet = 0.0

                if LWH_list[-1] > 121.0 or LWH_list[-2] > 76.0 or 330.0>=(L+2*(W+H))>266.0:
                    Over_lon = 10.45+5.39
                else:
                    Over_lon = 0.0

                if 419.0>=(L+2*(W+H))>330.0 or LWH_list[-1] > 243.0:
                    Over_Vol = 71.5+29.15
                    if Over_lon != 0.0 and FinalGW < 90:
                        FinalGW = 90
                else:
                    Over_Vol = 0.0

                house_fee = 3.85
                base_Fre = value.loc[value['Lbs.']==float(FinalGW),'Zone 8']

                base_Fre = float(base_Fre.iloc[0])

                add_up = base_Fre+Over_Vol+Over_Wet+Over_lon+house_fee
                Fuel_add = add_up*0.13
                freight = freight+Fuel_add+add_up
        except:
            freight = "NA"
        fre_list.append(freight)

    df['Fed1_Freight'] = fre_list

    df.to_csv(r'E:\OneDrive\露露\邮费.csv')

def main_ups():
    df1 = Get_Inven()
    value = Get_UPS_Price()
    Coun_UPS_Fre(df1,value)

def main_Fed1():
    df,df1 = Get_Inven1()
    value = Get_Fed1_price()
    Coun_Fed1_Fre(df1,value,df)

if __name__ == '__main__':
    main_Fed1()