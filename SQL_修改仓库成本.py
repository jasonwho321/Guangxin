import pyodbc
import pandas as pd

class Config:
    def __init__(self):
        self.server='47.119.164.123'
        self.database = '39F'
        self.username ='sa'
        self.password ='Aa12345678'

class DB:
    def __init__(self):
        config=Config()
        server=config.server
        database = config.database
        username =config.username
        password =config.password
        self.cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
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

if __name__ == '__main__':
    db = DB()
    # df2 = pd.read_excel(r"C:\Users\Admin\Documents\Tencent Files\544409644\FileRecv\211126 新品备货更新价格.xlsx")
    # print(df2)
    # for index, row in df2.iterrows():
    #     itemNumber = row['货号']
    #     Unit_Price = row['新价格\n(元/件）']
    #     print(row['货号'])
    #     query1 = f"UPDATE [dbo].[COGS] SET [Unit_Price] = {Unit_Price} WHERE [Item_Number] = '{itemNumber}'"
    #     query2 = f"UPDATE [dbo].[COGS 2] SET [Unit_Price] = {Unit_Price} WHERE [Item_Number] = '{itemNumber}'"
    #     db.run(query1)
    #     db.run(query2)
    query_export = 'SELECT * FROM [39F].[dbo].[COGS3] where 月 = 11'
    df1 = db.get_df(query_export)
    df1.to_csv(r'C:\Users\Admin\Documents\出仓成本_1126.csv',encoding="utf_8_sig")