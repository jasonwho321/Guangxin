import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from base import get_engine
def main():
    engine = get_engine()
    # 获取当前日期的前一天
    previous_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 查询销售数据
    sales_query = f"""
    SELECT *
    FROM Sales.Wayfair_Sales_with_Group_ID
    WHERE PO_Date = '{previous_day}'
    """
    sales_data = pd.read_sql(sales_query, engine)

    # 查询库存数据
    inventory_query = """
    SELECT *
    FROM Inv_Mgmt.Inventory_sum_latest_THTJ
    """
    inventory_data = pd.read_sql(inventory_query, engine)

    # 计算自营仓和平台仓出单量
    sales_data['自营仓出单量（对应平台）'] = np.where(sales_data['Warehouse_Name'].str.contains('CastleGate'), 0, sales_data['Quantity'])
    sales_data['平台仓出单量（对应平台）'] = np.where(sales_data['Warehouse_Name'].str.contains('CastleGate'), sales_data['Quantity'], 0)

    # 计算自营仓和平台仓库存量
    inventory_data['自营仓库存量（整个晓望）'] = np.where(inventory_data['Warehouse'].str.contains('CastleGate'), 0, inventory_data['total_qty'])
    inventory_data['平台仓库存量（整个晓望）'] = np.where(inventory_data['Warehouse'].str.contains('CastleGate'), inventory_data['total_qty'], 0)

    # 将销售数据按国家分组
    us_sales_data = sales_data[sales_data['Sales_Country'] == 'US']
    ca_sales_data = sales_data[sales_data['Sales_Country'] == 'CA']

    # 将库存数据按国家分组
    us_inventory_data = inventory_data[inventory_data['Site'] == 'US']
    ca_inventory_data = inventory_data[inventory_data['Site'] == 'CA']

    # 按 Group_ID 汇总销售数据
    us_sales_summary = us_sales_data.groupby(['Group_ID', 'Item_Number'], as_index=False).agg({'自营仓出单量（对应平台）': 'sum', '平台仓出单量（对应平台）': 'sum'})
    ca_sales_summary = ca_sales_data.groupby(['Group_ID', 'Item_Number'], as_index=False).agg({'自营仓出单量（对应平台）': 'sum', '平台仓出单量（对应平台）': 'sum'})

    # 按 Group_ID 汇总库存数据
    us_inventory_summary = us_inventory_data.groupby(['category','Group_ID'], as_index=False).agg({'自营仓库存量（整个晓望）': 'sum', '平台仓库存量（整个晓望）': 'sum'})
    ca_inventory_summary = ca_inventory_data.groupby(['category','Group_ID'], as_index=False).agg({'自营仓库存量（整个晓望）': 'sum', '平台仓库存量（整个晓望）': 'sum'})

    # 计算出单总数和库存总量
    us_sales_summary['出单总数（对应平台）'] = us_sales_summary['自营仓出单量（对应平台）'] + us_sales_summary['平台仓出单量（对应平台）']
    ca_sales_summary['出单总数（对应平台）'] = ca_sales_summary['自营仓出单量（对应平台）'] + ca_sales_summary['平台仓出单量（对应平台）']
    us_inventory_summary['库存总量（整个晓望）'] = us_inventory_summary['自营仓库存量（整个晓望）'] + us_inventory_summary['平台仓库存量（整个晓望）']
    ca_inventory_summary['库存总量（整个晓望）'] = ca_inventory_summary['自营仓库存量（整个晓望）'] + ca_inventory_summary['平台仓库存量（整个晓望）']

    # 根据 Group_ID 合并销售和库存数据
    us_merged_data = us_sales_summary.merge(us_inventory_summary, on='Group_ID', how='outer')
    ca_merged_data = ca_sales_summary.merge(ca_inventory_summary, on='Group_ID', how='outer')

    # 删除没有 HIB SKU 的数据
    us_merged_data = us_merged_data.dropna(subset=['Item_Number'])
    ca_merged_data = ca_merged_data.dropna(subset=['Item_Number'])

    # 将 HIB SKU 移动到第一列
    us_merged_data = us_merged_data.reset_index().set_index('Item_Number')
    ca_merged_data = ca_merged_data.reset_index().set_index('Item_Number')

    # 计算日均销量和周转天数
    us_merged_data['日均销量（对应平台）'] = us_merged_data['出单总数（对应平台）'] / 7  # 假设一个月30天
    us_merged_data['周转天数'] = np.where(us_merged_data['日均销量（对应平台）'] > 0, us_merged_data['库存总量（整个晓望）'] / us_merged_data['日均销量（对应平台）'], 0)

    ca_merged_data['日均销量（对应平台）'] = ca_merged_data['出单总数（对应平台）'] / 7  # 假设一个月30天
    ca_merged_data['周转天数'] = np.where(ca_merged_data['日均销量（对应平台）'] > 0, ca_merged_data['库存总量（整个晓望）'] / ca_merged_data['日均销量（对应平台）'], 0)

    # 添加空的 图片 和 出单分析/补货分析/折扣申请 列
    us_merged_data['图片'] = ''
    us_merged_data['出单分析/补货分析/折扣申请'] = ''
    ca_merged_data['图片'] = ''
    ca_merged_data['出单分析/补货分析/折扣申请'] = ''

    # 将 图片 列移动到 HIB SKU 之后
    us_merged_data = us_merged_data.reset_index().set_index(['Item_Number', '图片','category'])
    ca_merged_data = ca_merged_data.reset_index().set_index(['Item_Number', '图片','category'])


    us_merged_data = us_merged_data.drop(['index','Group_ID'], axis=1)
    ca_merged_data = ca_merged_data.drop(['index','Group_ID'], axis=1)

    # 创建ExcelWriter
    output_file = f"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/每日出单产品分析/【Wayfair】{datetime.now().strftime('%Y%m%d')} 每日出单产品分析.xlsx"
    with pd.ExcelWriter(output_file) as writer:
        us_merged_data.to_excel(writer, sheet_name='US')
        ca_merged_data.to_excel(writer, sheet_name='CA')
if __name__ == '__main__':
    main()