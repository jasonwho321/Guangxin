import pandas as pd
import os
import csv
keyword = ['collapsed','injur','hurt','fall','fell']
g = os.walk(r"E:\OneDrive\广新\售后报告\Incident report")
i = 1
full_path_list = []
for path,dir_list,file_list in g:
    for file_name in file_list:
        full_path = os.path.join(path, file_name)
        if 'photos' in str(file_name) and 'csv' in str(file_name):
            print(full_path)
            full_path_list.append(full_path)
        i+=1
print(full_path_list)
PO_list = []
SKU_list = []
for path in full_path_list:
    df = pd.read_csv(path)
    print(path)
    print(df.head())
    for index,row in df.iterrows():
        for k in keyword:
            if k in str(row['Comment']):
                print(index)
                PO_list.append(row['PO Number'])
                SKU_list.append(row['Part Number'])
data = {'PO Number':PO_list,'Part Number':SKU_list}
out_put = pd.DataFrame(data)
out_put.to_csv(r'E:\OneDrive\广新\售后报告\Incident report\output.csv')




