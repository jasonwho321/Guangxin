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

def main(dir):
    today = datetime.strftime(datetime.today(), '%Y-%m-%d')

    df = pd.read_excel(dir)


    # record date uploaded
    df['date_upload'] = today

    #####  upload module  #####
    db = DB()

    #####  download headers  #####
    db_header = db.get_df('SELECT TOP(1) * FROM [stock_age];').columns
    header = '],['.join(db_header)
    values = '?,' * len(db_header)

    #####  delete table  #####
    db.run('DELETE FROM [stock_age];')

    #####  insert into db  #####
    print('Now insert values')
    # insert to Database
    t0 = datetime.now()
    for index, row in df.iterrows():
        db.cursor.execute(f"INSERT INTO [stock_age] ([{header}]) values({values[:-1]});",
                          row['SKU'],
                          row['SKU Name'],
                          row['Customer'],
                          row['Warehouse'],
                          row['Country'],
                          row['Unit Volume(CBM)'],
                          row['0-90'],
                          row['91-180'],
                          row['181-270'],
                          row['271-365'],
                          row['>365'],
                          row['date_upload']
                          )
    db.cursor.commit()

    t1 = datetime.now()
    print('\nTime used: ', t1 - t0)

    db.disconnect()
    pass


def run():
    dir = r'C:\Users\Admin\Downloads\晓望集群库龄2022-07-13.xlsx'
    main(dir)


if __name__ == '__main__':
    run()
