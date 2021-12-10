import pandas as pd
import os
from datetime import datetime
g = os.walk(r"C:\Users\Admin\PycharmProjects\pythonProject\Scrapy\Files")
i = 1
full_path_list = []
for path,dir_list,file_list in g:
    for file_name in file_list:
        full_path = os.path.join(path, file_name)
        if 'Price' in str(file_name):
            print(full_path)
            full_path_list.append(full_path)
        i+=1
print(full_path_list)
results = pd.DataFrame(columns=('W_sku','list_price','sale_price','date'))
for path in full_path_list:
    df = pd.read_csv(path,header=None)
    df.rename(columns={0:'W_sku',1:'list_price',2:'sale_price'})
    date = datetime.strptime(str(path[-12:-4]),'%Y%m%d').strftime('%Y/%m/%d')
    print(date)
    df['date'] = date
    results = results.append(df)
results.to_csv(r'E:\OneDrive\广新\价格汇总.csv')