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
    query1 = f"UPDATE [dbo].[COGS 2] SET [Unit_Price] = {Unit_Price} WHERE [Item_Number] = '{itemNumber}'"
    query2 = f"UPDATE [dbo].[COGS 2] SET [Unit_Price] = {Unit_Price} WHERE [Item_Number] = '{itemNumber}'"
    db.run(query1)
    db.run(query2)
    # query_export1 = 'SELECT * FROM [39F].[dbo].[COGS]'
    # query_export2 = 'SELECT * FROM [39F].[dbo].[COGS 2]'
    # df1 = db.get_df(query_export1)
    # df2 = db.get_df(query_export2)
    # df1.to_csv(r'C:\Users\Admin\Documents\COGS.csv',encoding="utf_8_sig")
    # df2.to_csv(r'C:\Users\Admin\Documents\COGS 2.csv', encoding="utf_8_sig")

    # query2 = "select * from dbo.WF2020_US union all select * from dbo.[Wayfair_US_Sales]"
    # df1 = db.get_df(query2)
    # print(df1)
    # df1.to_csv('output')
    # columns = [column for column in df]
    # for column in columns:
    #     column1 = column.replace(" ","_")
    #     print(column1)
    #     query = f"EXEC sp_rename 'dbo.WF2020_US.{column}', '{column1}', 'COLUMN';"
    #
    #     print(query2)
    #     try:
    #         db.run(query2)
    #     except Exception:
    #         print(Exception)
    #         pass