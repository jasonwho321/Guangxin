import random
from requests.cookies import cookiejar_from_dict
import time
import io
import zipfile
import pandas as pd
from io import BytesIO
import pyodbc
from sqlalchemy import create_engine
import json
import requests
from base import bot_push_text
from datetime import datetime, timedelta,date
from base import get_system_path
# 写一个随机生成ua的函数
def ua_generator():
    user_agent_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",
        "Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9a8) Gecko/2007100600 Firefox/1.0 (Swiftfox)",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2.21pre) Gecko/20110101 Firefox/3.6.21pre",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
        "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:46.0) Gecko/20100101 Firefox/46.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
        "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:44.0) Gecko/20100101 Firefox/44.0",
        "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:44.0) Gecko/20100101 Firefox/44.0",
        "Mozilla/5.0 (Windows NT 6.1; rv:43.0) Gecko/20100101 Firefox/43.0",
        "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
        "Mozilla/5.0 (Windows NT 6.3; rv:43.0) Gecko/20100101 Firefox/43.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0"]
    return random.choice(user_agent_list)

def vartoflo(df,table_name):
    server = config_data['server']
    database = config_data['database']
    user = config_data['user']
    password = config_data['password']
    # 创建数据库连接字符串
    driver_path =config_data['driver_path']
    conn_str = (
        f"Driver={{{driver_path}}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={user};"
        f"PWD={password};"
    )

    # 创建SQLAlchemy引擎
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

    # 查询所有float类型的列
    query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{table_name}' AND data_type = 'float'
    """
    float_columns = pd.read_sql(query, engine)

    # 然后，你可以在DataFrame中找到这些列，并转换它们的类型
    for column in float_columns['column_name']:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')
    return df
def read_config(config_file_path):
    with open(config_file_path, 'r') as f:
        config_data = json.load(f)
    return config_data
# 读取配置文件
config_file_path = get_system_path('SQLserver')  # 根据实际情况调整路径
config_data = read_config(config_file_path)

def get_servertoken(config_file_path = get_system_path('airy_hib_account')):
      # 根据实际情况调整路径
    config_data = read_config(config_file_path)
    url = "https://sso.houseinbox.com/authorize/sso/doLogin"

    # 使用提供的用户名和密码
    username = config_data['username']
    password = config_data['password']
    cook_userNumber = config_data['cook_userNumber']
    cook_userid= config_data['cook_userid']

    # 为了安全起见，建议对密码进行散列处理
    import hashlib
    hashed_password = hashlib.sha512(password.encode()).hexdigest()

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://sso.houseinbox.com",
        "referer": "https://sso.houseinbox.com/?logout=ssoLogout&callback=https%3A%2F%2Fhouseinbox.com%2Fusercenter",
        "sec-ch-ua": '"Chromium";v="112", "Microsoft Edge";v="112", "Not:A-Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48",
    }
    loginTime = int(time.time() * 1000)
    payload = {
        "user": {"aname": username, "apassword": hashed_password},
        "username": username,
        "password": hashed_password,
        "loginTime": loginTime,
        "targetDomain": "houseinbox.com",
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    # 检查登录是否成功
    if response.status_code == 200:
        json_response = response.json()
        cookies = {'cook_ctxComplete': 'true', 'cook_isLogin': 'true', 'cook_userid': cook_userid,
                   'cook_userName': username, 'cook_userNumber': cook_userNumber,
                   'cook_serviceToken': json_response['serviceToken'],
                   'cook_loginTime': str(loginTime), 'cook_random': json_response['random']}
        url = 'https://sso.doccenter.net/authorize/sso/crossSiteCookie?targetDomain=doccenter.net&userId={}&random={}'.format(cook_userid,json_response['random'])
        cookies = cookiejar_from_dict(cookies)
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5',
            'referer': 'https://houseinbox.com/',
            'sec-ch-ua': '"Chromium";v="112", "Microsoft Edge";v="112", "Not:A-Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'script',
            'sec-fetch-mode': 'no-cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48'
        }

        response = requests.get(url, headers=headers, cookies=cookies)

        if response.status_code == 200:
            # 提取 cook_serviceToken
            cookies = response.cookies
            service_token = cookies.get("cook_serviceToken")
            return [json_response['serviceToken'],service_token]
        else:
            print("Error retrieving the URL.")
    else:
        print("请求失败")
def upload_to_sql_server(df, table_name, timestamp_column,schema, if_exists='replace',is_time=True):
    # 可以选择'replace'或'append_no_duplicates'
    # 从配置文件中获取所需的值
    server = config_data['server']
    database = config_data['database']
    user = config_data['user']
    password = config_data['password']
    # 创建数据库连接字符串
    driver_path =config_data['driver_path']
    conn_str = (
        f"Driver={{{driver_path}}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={user};"
        f"PWD={password};"
    )

    # 创建SQLAlchemy引擎
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

    # 连接到数据库
    conn = pyodbc.connect(conn_str)
    if if_exists == 'replace':
        df.to_sql(table_name, engine, if_exists="replace", index=False,schema=schema)
        print(f"Data uploaded to table '{table_name}' in database '{database}' on server '{server}'.")
    elif if_exists == 'append_no_duplicates':
        if is_time:
            # 获取上传表格中的时间范围
            min_timestamp = df[timestamp_column].min()
            max_timestamp = df[timestamp_column].max()

            # 删除与上传表格中的时间范围重叠的数据
            query = f"DELETE FROM {schema}.{table_name} WHERE {timestamp_column} >= '{min_timestamp}' AND {timestamp_column} <= '{max_timestamp}'"
            conn.execute(query)
            conn.commit()
        else:
            unique_str_values = df[timestamp_column].unique()
            # 将这些值转换为适当的 SQL 查询格式
            formatted_str_values = ', '.join([f"'{value}'" for value in unique_str_values])

            # 删除与上传表格中的字符串值匹配的数据
            delete_query = f"DELETE FROM {schema}.{table_name} WHERE [{timestamp_column}] IN ({formatted_str_values})"
            conn.execute(delete_query)
            conn.commit()

        # 将新数据追加到数据库表格中
        df.to_sql(table_name, engine, if_exists="append", index=False,schema=schema)
        print(f"Data uploaded to table '{table_name}' in database '{database}' on server '{server}' without duplicates.")
    else:
        print(f"Error: Invalid 'if_exists' value. Allowed values are 'replace' and 'append_no_duplicates'.")

    # 关闭数据库连接
    conn.close()
def download_upload_transportInfo():
    table_name = 'China_WMS'
    schema = 'Transport'

    timestamp_column = 'ETD'

    url = "https://chinawms.houseinbox.com/rbcs/transport/exportData"
    headers = {'userid': '958'}
    payload = {
        "transportQuery": {
            "ainstitutionAids": [736, 961, 929, 962, 450, 998, 298, 939, 332, 976, 977, 375, 633, 765, 605]
        }
    }

    # 发送请求
    response = requests.post(url, json=payload, headers=headers)
    # 检查响应状态
    if response.status_code == 200:
        # 将数据读入pandas DataFrame
        try:
            df = pd.read_excel(response.content,engine='openpyxl')
            df = df[(df['业务类型'] == '电商') & (df['柜号'].notnull()) & (df['柜号'].str.len() == 11)]
            print(df)
            upload_to_sql_server(df, table_name, timestamp_column,schema)
        except Exception as e:
            bot_push_text(f"中国仓wms数据报错，报错信息如下：\n{e}",['18062351119'])
    else:
        bot_push_text(f"中国仓wms数据下载失败，状态码："+str(response.status_code), ['18062351119'])

def download_upload_inbound():
    # 获取当前日期
    today = datetime.now().date()

    # 计算前30天的日期
    thirty_days_ago = today - timedelta(days=45)

    # 将日期格式化为字符串
    date_str = thirty_days_ago.strftime("%Y-%m-%d")
    table_name = 'Inbound_Record'
    schema = 'Transport'

    timestamp_column = '创建时间'

    url = "https://www.doccenter.net/datacenter/storagecharges/exportInboundFee"
    beginTime= f"{date_str}T16:00:00.000Z"
    with open('/Users/huzhang/PycharmProjects/Guangxin/config_file/inbound_payload.json', 'r') as file:
      payload = json.load(file)
    # 发送请求
    payload['aquery']['beginTime'] = beginTime
    response = requests.post(url, json=payload)
    # 检查响应状态
    if response.status_code == 200:
        # 将数据读入pandas DataFrame
        try:
            # 将响应内容转换为字符串类型，并将其传递给StringIO构造函数
            content_str = str(response.content, encoding='gb18030')
            # 将StringIO对象传递给pd.read_csv方法
            df = pd.read_csv(io.StringIO(content_str))
            df_new = df[['Warehouse', 'Inbound Date', 'Container Number', 'Name', 'Sku', 'Remaining Inventory','Create Date']]
            df_new = df_new.rename(columns={'Warehouse': '仓库',
                                            'Inbound Date': '入库时间',
                                            'Container Number': '柜号',
                                            'Name': '英文名',
                                            'Sku': 'SKU',
                                            'Remaining Inventory': '入库数量',
                                            'Create Date': '创建时间'})

            # 修改SKU列的数据类型为文本
            df_new['SKU'] = df_new['SKU'].astype(str)
            upload_to_sql_server(df_new, table_name, timestamp_column,if_exists='append_no_duplicates',schema=schema)
        except Exception as e:
            bot_push_text(f"自营仓入库数据报错，报错信息如下：\n{e}",['18062351119'])
    else:
        bot_push_text(f"自营仓入库数据下载失败，状态码："+str(response.status_code), ['18062351119'])

def download_upload_trans_inv():

  table_name = 'Trans_between_Operation'
  schema = 'Transport'

  timestamp_column = '完成时间'
  # 设置请求头和负载
  headers = {
    "servicetoken": get_servertoken()[1],
    "userid": "485",
    "currentdomain": "doccenter.net"
  }

  payload = {
  "aquery": {
    "pageDomain": {
      "pageIndex": 1,
      "pageSize": 10,
      "recordCount": 48,
      "startIndex": 0
    },
    "pageFourunitDomain": {
      "pageIndex": 1,
      "pageSize": 10,
      "recordCount": 26,
      "startIndex": 0
    },
    "ifInitFalg": 1,
    "topType": "39集团",
    "pageSaleunitDomain": {
      "pageIndex": 1,
      "pageSize": 10,
      "recordCount": 163,
      "startIndex": 0
    },
    "saleifInitFalg": 1
  }
}

  # 下载ZIP文件
  url = "https://www.doccenter.net/datacenter/legalSixReport/xaz/downloadNewestReport"  # 请将这里替换为ZIP文件的下载链接
  response = requests.get(url, headers=headers, json=payload)
  # 检查响应状态
  if response.status_code == 200:
    # 将数据读入pandas DataFrame
    try:
      # 解压ZIP文件并读取以"report物权转移单据明细"开头的.xlsx文件
      with zipfile.ZipFile(BytesIO(response.content)) as zfile:
        for file in zfile.namelist():
          if file.startswith("report物权转移单据明细") and file.endswith(".xlsx"):
            with zfile.open(file) as xlsx_file:
              df = pd.read_excel(xlsx_file, engine='openpyxl')
              df.columns = [col.replace(" ","").replace("\n","") for col in df.columns]
              upload_to_sql_server(df, table_name, timestamp_column, if_exists='append_no_duplicates',schema=schema)
    except Exception as e:
            bot_push_text(f"移库数据报错，报错信息如下：\n{e}",['18062351119'])
  else:
      bot_push_text(f"移库数据下载失败，状态码："+str(response.status_code), ['18062351119'])

def download_upload_relative_merch():
  table_name = 'RelativeMerchandise'
  schema = "Inv_Mgmt"
  timestamp_column = ''
  with open('/Users/huzhang/PycharmProjects/Guangxin/config_file/relative_merch_payload.json', 'r') as file:
    payload = json.load(file)
  url = 'https://houseinbox.com/bshop/ecaccountRelativeMerchandise/exportData'
  response = requests.get(url, json=payload)
  # 检查响应状态
  if response.status_code == 200:
    # 将数据读入pandas DataFrame

    try:
        with io.BytesIO(response.content) as f:
            df = pd.read_excel(f, engine='xlrd')

        upload_to_sql_server(df, table_name, timestamp_column,schema=schema)
    except Exception as e:
            bot_push_text(f"相关货号映射报错，报错信息如下：\n{e}",['18062351119'])
  else:
      bot_push_text(f"相关货号映射下载失败，状态码："+str(response.status_code), ['18062351119'])

def download_upload_merch():
  table_name = 'Merchandise'
  schema = "Inv_Mgmt"
  timestamp_column = 'Last Update Date'
  with open('/Users/huzhang/PycharmProjects/Guangxin/config_file/merch_payload.json', 'r') as file:
    payload = json.load(file)
  url = 'https://houseinbox.com/bshop/thirdmerchandiseMap/xaz/hibService2/exportMerchadiseMap'
  headers = {
    "servicetoken": get_servertoken('/Users/huzhang/PycharmProjects/Guangxin/config_file/HIB_login_XWMG.json')[0],
    "userid": "1665",
    "currentdomain": "houseinbox.com"
  }
  response = requests.get(url, json=payload,headers=headers)
  # 检查响应状态
  if response.status_code == 200:
    # 将数据读入pandas DataFrame

    try:
        with io.BytesIO(response.content) as f:
            df = pd.read_excel(f, engine='openpyxl')
        upload_to_sql_server(df, table_name, timestamp_column,schema=schema,if_exists='append_no_duplicates')
    except Exception as e:
            bot_push_text(f"货号映射报错，报错信息如下：\n{e}",['18062351119'])
  else:
      bot_push_text(f"货号映射下载失败，状态码："+str(response.status_code), ['18062351119'])

def download_upload_inv():
    table_name = 'Inventory_Records'
    schema = "Inv_Mgmt"
    timestamp_column = 'upload_date'
    link = 'https://houseinbox.com/bshop/forestMerchandise/hibService2/exportInventorycsv'
    headers = {"userid": "485",
               "cookie": "cook_warehouse=20190428000002"}
    response = requests.get(url=link, headers=headers)
    if response.status_code == 200:
        # 将数据读入pandas DataFrame
        try:
            # 将响应内容转换为字符串类型，并将其传递给StringIO构造函数
            content_str = str(response.content, encoding='gb18030')
            # 将StringIO对象传递给pd.read_csv方法
            df = pd.read_csv(io.StringIO(content_str))
            df['upload_date'] = date.today().strftime('%Y-%m-%d')
            df = df[['Warehouse','Site','Organization','SKU','InventoryCode','Goods Name','Available Qty','Lock Qty','Damaged Qty','upload_date']]
            upload_to_sql_server(df, table_name, timestamp_column,schema=schema,if_exists='append_no_duplicates')
        except Exception as e:
            bot_push_text(f"自营仓库存数据报错，报错信息如下：\n{e}",['18062351119'])
    else:
        bot_push_text(f"自营仓库存数据下载失败，状态码："+str(response.status_code), ['18062351119'])

def upload_cg_inbound():
    table_name = 'CG_Inbound_Records'
    schema = "Transport"
    timestamp_column = 'Original Order Id'
    df = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/CG入库记录_US.csv'))
    upload_to_sql_server(df, table_name, timestamp_column,schema=schema,if_exists='append_no_duplicates',is_time=False)

def upload_cg_inv():
    table_name = 'CG_Inventory_Records'
    schema = "Inv_Mgmt"
    timestamp_column = 'upload_date'
    df_us = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/CG库存_US.csv'))
    df_ca = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/CG库存_CA.csv'))
    df_us['Country'] = 'US'
    df_ca['Country'] = 'CA'

    df = pd.concat([df_us,df_ca],ignore_index=True)
    df['upload_date'] = date.today().strftime('%Y-%m-%d')
    upload_to_sql_server(df, table_name, timestamp_column,schema=schema,if_exists = 'append_no_duplicates')

def upload_cg_ful():
    schema = "Finance"

    table_name_ful = 'CG_Fulfillment'
    table_name_media = 'CG_Media'
    table_name_mer = 'CG_Merchandising'
    table_name_trans = 'CG_Transportation'

    timestamp_column_ful = 'Charge Date'
    timestamp_column_media = 'createDate'
    timestamp_column_mer = 'createDate'
    timestamp_column_trans = 'Charge Date'

    df_ful_us = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Fulfillment_US.csv'))
    df_ful_ca = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Fulfillment_CA.csv'))
    df_trans_us = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Transportation_US.csv'))
    df_trans_ca = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Transportation_CA.csv'))
    df_media = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/Media.csv'))
    df_mer = pd.read_csv(get_system_path('/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/Merchandising.csv'))

    # 使用rename方法和lambda函数去掉 '(USD)'
    df_ful_us.columns = df_ful_us.columns.map(lambda x: x.replace(' (USD)', ''))
    df_ful_ca.columns = df_ful_ca.columns.map(lambda x: x.replace(' (CAD)', ''))


    df_ful = pd.concat([df_ful_us,df_ful_ca],ignore_index=True)
    df_trans = pd.concat([df_trans_us,df_trans_ca],ignore_index=True)
    df_ful = vartoflo(df_ful,table_name_ful)
    df_trans = vartoflo(df_trans, table_name_trans)
    df_mer = df_mer.drop(columns=['discounts', 'taxes'])
    upload_to_sql_server(df_ful, table_name_ful, timestamp_column_ful,schema=schema,if_exists = 'append_no_duplicates',is_time=False)
    upload_to_sql_server(df_trans, table_name_trans, timestamp_column_trans,schema=schema,if_exists = 'append_no_duplicates',is_time=False)
    upload_to_sql_server(df_media, table_name_media, timestamp_column_media,schema=schema,if_exists = 'append_no_duplicates',is_time=False)
    upload_to_sql_server(df_mer, table_name_mer, timestamp_column_mer,schema=schema,if_exists = 'append_no_duplicates',is_time=False)

if __name__ == '__main__':
    upload_cg_ful()