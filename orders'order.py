import xlwings as xw
from time import sleep

import csv
import pandas as pd
file_path = r"C:\Users\Admin\Downloads\WF_US_Sales.csv"

df = pd.read_csv(file_path)
df_order_list = ['Warehouse Name',
'Store Name',
'PO Number',
'PO Date',
'Must Ship By',
'Backorder Date',
'Order Status',
'Item Number',
'Item Name',
'Quantity',
'Wholesale Price',
'Ship Method',
'Carrier Name',
'Shipping Account Number',
'Ship To Name',
'Ship To Address',
'Ship To Address 2',
'Ship To City',
'Ship To State',
'Ship To Zip',
'Ship To Phone',
'Inventory at PO Time',
'Inventory Send Date',
'Ship Speed',
'PO Date & Time',
'Registered Timestamp',
'Customization Text',
'Event Name',
'Event ID',
'Event Start Date',
'Event End Date',
'Event Type',
'Backorder Reason',
'Original Product ID',
'Original Product Name',
'Event Inventory Source',
'Packing Slip URL',
'Tracking Number',
'Ready for Pickup Date',
'SKU',
'Destination Country',
'Depot ID',
'Depot Name',
'Wholesale Event Source',
'Wholesale Event Store Source',
'B2BOrder',
'Composite Wood Product',
'Sales Channel',
]
df = df[df_order_list]
print(df)
df.to_csv(file_path,index=False)


# app = xw.App(visible=True,add_book=False)
# wb = app.books.open(file_path)
# sht1 = wb.sheets[0]
# sht1.api.Rows(1).Insert()
# sleep(5)
#
# sht1.range('a1').value=order_list
# sleep(5)
# cell = sht1.used_range.last_cell
# rows = cell.row
# columns = cell.column
#
# sht1.range('a2',(rows,columns)).api.Sort(Key1=sht1.range('A1').api, Order1=1,Orientation=1)
# sleep(5)
# sht1.range('a1').api.EntireRow.Delete()
