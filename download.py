import requests
import pandas as pd
import io
from datetime import datetime, timedelta
import json
from database.upload_database import get_servertoken
# 读取 JSON 文件并转换为字典
with open('/Users/huzhang/Downloads/jsoncrack (1).json', 'r') as file:
    payload = json.load(file)
url = "https://base.houseinbox.com/base/controller/merchandise/hibService2/exportData"

# 设置筛选条件的起始日期
start_date = datetime.strptime("2021-03-01", "%Y-%m-%d")

# 定义一个时间段（天数），用于分批次获取数据
time_interval = timedelta(days=5)

# 用于存储所有数据的DataFrame
all_data = pd.DataFrame()
headers = {
    "servicetoken": get_servertoken()[1],
    "userid": "485",
    "currentdomain": "houseinbox.com",
    }
while True:
  # 构建payload
    payload["aquery"]["createtimefrom"] = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payload["aquery"]["createtimeto"] = (start_date + time_interval).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    # 发送POST请求
    print(payload["aquery"]["createtimefrom"])
    response = requests.post(url, json=payload,headers=headers)
    # 检查响应状态
    if response.status_code == 200:
        try:
            with io.BytesIO(response.content) as f:
                current_data = pd.read_excel(f,engine='xlrd')
            # 将响应内容（二进制数据）转换为DataFrame
            print(len(current_data))
            # 将当前批次的数据追加到all_data DataFrame中
            all_data = pd.concat([all_data, current_data], ignore_index=True)

            # 更新createtimefrom，准备获取下一个时间段的数据
            start_date += time_interval

            # 检查是否已经获取了所有数据
            if len(current_data) == 0:
                break
        except Exception as e:
            print(f"Error occurred while processing the response: {e}")
            break
    else:
        print(f"Request failed with status code {response.status_code}.")
        break

# 在此处处理all_data，例如将其保存到CSV文件中
all_data.to_csv('output.csv',encoding='gb18030')