import pandas as pd

# 读取原始数据
data_file1 = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/成本到箱/成本到箱(每箱).csv'
data_file2 = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/成本到箱/库存汇总.csv'
output_file = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/成本到箱/DDP-出库成本_1206.csv'
output_file2 = '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/成本到箱/成本到箱(一箱一行).csv'

df = pd.read_csv(data_file1)

# 创建一个新列，其中包含重复的行索引
df['repeat_index'] = df.apply(lambda row: [row.name] * row['入库数量'], axis=1)

# 使用 explode 函数将行按照 repeat_index 列展开
new_df = df.explode('repeat_index').reset_index(drop=True)

# 删除 repeat_index 列
new_df = new_df.drop(columns=['repeat_index'])

# 打印拆分后的数据
new_df['入库数量'] = 1

df2 = pd.read_csv(data_file2)

merged_table = pd.merge(df2, new_df, on=['国家', '仓库类型', '英文名'])

# 将 '入库时间' 列转换为 datetime 类型
merged_table['入库时间'] = pd.to_datetime(merged_table['入库时间'])
# 按照货物名称和入库时间进行排序
sorted_table = merged_table.sort_values(['国家', '仓库类型', '英文名', '入库时间排序'], ascending=[True, True, True, True])


# 按照货物名称和入库时间进行分组，并计算每个分组的累计数量
sorted_table['cumulative_quantity'] = sorted_table.groupby(['国家', '仓库类型', '英文名'])['入库数量'].cumsum()

# 提取满足条件的数据行
selected_rows = sorted_table[sorted_table['cumulative_quantity'] <= sorted_table['在仓库存']]

selected_rows.to_csv(output_file2, encoding='GB18030')

# 计算每个分组内满足条件的数据行的平均成本
average_cost_by_item_final = selected_rows.groupby(['国家', '仓库类型', '英文名'])[['出库成本', 'DDP', '单套仓租']].mean()

average_cost_by_item_final['出库成本(不含仓租)'] = average_cost_by_item_final['出库成本'] - average_cost_by_item_final['单套仓租']

df_avgcost_final = pd.DataFrame(average_cost_by_item_final)

df_avgcost_final.to_csv(output_file, encoding='GB18030')