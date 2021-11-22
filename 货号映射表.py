import xlwings as xw
import pandas as pd
import csv

# 打开要修改格式的csv文件 data1,并读出内容到reader


if __name__ == '__main__':
    path = r'C:\Users\Admin\PycharmProjects\pythonProject\thirdMerchandiseMap (3).csv'
    # app = xw.App(visible=True, add_book=False)
    # wb = app.books.open(path)
    # sht = wb.sheets[0]
    # print(str(sht.cells.last_cell.row))
    with open(path, 'r', encoding='ANSI') as f:
        reader = csv.reader(f)
        for line in reader:
            # 将data1的内容以utf8格式写入新的csv文件data2
            with open('./thirdMerchandiseMap.csv', 'a', encoding='utf-8-sig') as d:
                writer = csv.writer(d)
    df = pd.read_csv('./thirdMerchandiseMap.csv')
    print(df.to_string())