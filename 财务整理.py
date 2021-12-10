import pandas as pd

df_CA = pd.read_csv(r'E:\OneDrive\广新\财务流水\2021_CA_Wayfair(1).csv')
df_US = pd.read_csv(r'E:\OneDrive\广新\财务流水\2021_US_Wayfair(1).csv')
df_wh = pd.read_excel(r'E:\OneDrive\广新\财务流水\122107 仓库映射表 - updated.xlsx')
df_name = pd.read_excel(r'E:\OneDrive\广新\财务流水\211015 更新映射表 （非套装映射）.xlsx')
df_fob = pd.read_excel(r'E:\OneDrive\广新\财务流水\FOB.xlsx')
print(df_fob.columns)
df_fob1 = df_fob[['物料英文名','每件FOB销售单价（USD）','40HQ 装柜量（件）','港口']]
df_CA['country']='CA'
df_US['country']='US'
df_all = pd.concat([df_US,df_CA],axis=0)

data_df = df_all['Item_Number'].str.split('+', expand=True)
column_name = ['Item_Number1','Item_Number2','Item_Number3']
data_df.columns = column_name
df_all1 = pd.concat([df_all,data_df], axis=1)


df_all2 = df_all1.merge(df_wh,left_on='Warehouse_Name',right_on='Warehouse_Name',how='left')
df_all3 = df_all2.merge(df_name,left_on='Item_Number1',right_on='Platform SKU',how='left')
df_all4 = df_all3.merge(df_fob1,left_on='Goods Name',right_on='物料英文名',how='left')

print(df_all4)
print(df_all4.columns)

df_all4.to_csv(r'E:\OneDrive\广新\财务流水\汇总表.csv',encoding='utf_8_sig',index=False)