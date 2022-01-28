import numpy as np
from datetime import datetime
import pandas as pd
import pyodbc
class Config:
    def __init__(self):
        self.server='iZ1wll9swpj1hbZ\SQLEXPRESS'
        self.database = '39F'
        self.username ='harryyan'
        self.password ='Aa12345678'
class EXC:
    def __init__(self):
        self.aaa = 0
class EXC_WF_SALE_DOWNLOAD(EXC):
    def __init__(self,country):
        EXC.__init__(self)
        source_path = 'E://OneDrive//广新//四象限//uploaded_files//'
        if country == 'US':
            print("reading US excel file")
            self.USdata = pd.read_csv(source_path + 'WF_US_Sales.csv')
            if len(self.USdata.columns) == 47:
                self.USdata['Sales_Channel'] = None
            # data cleaning
            self.USdata['Item Number'] = self.USdata.apply(lambda x: str(x['Item Number']).strip('="'), axis = 1)
            self.USdata.replace("No Feed","",inplace=True)
        else:
            print("reading CA excel file")
            self.CAdata = pd.read_csv(source_path + 'WF_CA_Sales.csv')
            if len(self.CAdata.columns) == 47:
                self.CAdata['Sales_Channel'] = None
            # data cleaning
            self.CAdata['Item Number'] = self.CAdata.apply(lambda x: str(x['Item Number']).strip('="'), axis = 1)
            self.CAdata.replace("No Feed","",inplace=True)
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

class WF_SALE(DB):
    def __init__(self):
        DB.__init__(self)
        self.header = self.get_df('SELECT TOP(1) * FROM [Wayfair_US_Sales];').columns
        self.names = '],['.join(self.header)
        self.values = '?,'*len(self.header)

    def insert(self,df,table):
        for index, row in df.iterrows():
            self.cursor.execute(f"INSERT INTO {table} ([{self.names}]) values({self.values[:-1]});",
                              row['Warehouse_Name'],
                              row['Store_Name'],
                              row['PO_Number'],
                              row['PO_Date'],
                              row['Must_Ship_By'],
                              row['Backorder_Date'],
                              row['Order_Status'],
                              row['Item_Number'],
                              row['Item_Name'],
                              row['Quantity'],
                              row['Wholesale_Price'],
                              row['Ship_Method'],
                              row['Carrier_Name'],
                              row['Shipping_Account_Number'],
                              row['Ship_To_Name'],
                              row['Ship_To_Address'],
                              row['Ship_To_Address_2'],
                              row['Ship_To_City'],
                              row['Ship_To_State'],
                              row['Ship_To_Zip'],
                              row['Ship_To_Phone'] if row['Ship_To_Phone'] is not np.nan else None,
                              row['Inventory_at_PO_Time'] if type(row['Inventory_at_PO_Time']) is int else None,
                              row['Inventory_Send_Date'],
                              row['Ship_Speed'],
                              row['PO_Date_&_Time'],
                              row['Registered_Timestamp'],
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              None,
                              row['Packing_Slip_URL'],
                              row['Tracking_Number'],
                              row['Ready_for_Pickup_Date'],
                              row['SKU'],
                              row['Destination_Country'],
                              None,
                              None,
                              None,
                              None,
                              row['B2BOrder'],
                              row['Composite_Wood_Product'],
                              None
                              )


def run_app():
    # initialization
    exc = EXC_WF_SALE_DOWNLOAD('US')
    db = WF_SALE()

    # find timeframe
    date_us_max = max(exc.USdata['PO Date'].astype('datetime64')).strftime('%Y-%m-%d')
    date_us_min = min(exc.USdata['PO Date'].astype('datetime64')).strftime('%Y-%m-%d')

    print('To replace time frame: ')
    print(f'US: from {date_us_min} to {date_us_max}\n')

    # delete from tables in db
    print('US to delete related timeframe:\n')
    db.run(f"DELETE FROM [Wayfair_US_Sales] WHERE [PO_Date] between '{date_us_min}' and '{date_us_max}';")

    # Syn headers
    exc.USdata.columns = db.header
    # clean data
    exc.USdata = exc.USdata.fillna(np.nan).replace([np.nan], [None])
    # insert to Database
    t0 = datetime.now()
    db.insert(exc.USdata, '[Wayfair_US_Sales]')
    print('insert')
    db.commit()
    print('commit')
    t1 = datetime.now()
    print('Time used: ', t1-t0)


if __name__ == '__main__':
    run_app()

