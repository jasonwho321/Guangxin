import pandas as pd
from base import get_engine
from tqdm import tqdm

# 创建数据库连接
engine_mysql = get_engine('/Users/huzhang/PycharmProjects/Guangxin/database/MySQL_wms.json',db_type='mysql')
engine_sqlserver = get_engine()

# 从MySQL数据库读取数据
df = pd.read_sql("SELECT * FROM ka_material", engine_mysql)

# 设置chunksize，这意味着每次将1000行数据写入SQL Server数据库
chunksize = 1000
num_chunks = (len(df) // chunksize) + 1

# 使用tqdm创建进度条
with tqdm(total=num_chunks) as pbar:
    for i in range(0, len(df), chunksize):
        df[i:i+chunksize].to_sql("material", engine_sqlserver, if_exists='append',schema="Product_Mgmt")
        pbar.update()  # 更新进度条

