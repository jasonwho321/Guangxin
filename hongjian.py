import csv
import io
import json
from typing import Dict, Any

from requests.cookies import cookiejar_from_dict
import pandas as pd
import requests
import datetime
import time

class Abnormal_Withdrawal:
    def __init__(self):
        self.config_data = {"username": "Bronzes.Hu@eriabank.com", "password": "hwx@11111111", "cook_userid": "2215",
                            "cook_userNumber": "20230726000001"}
        self.userid = '2215'

    def filter_out_null_values(self, json_dict):
        if isinstance(json_dict, dict):
            return {k: self.filter_out_null_values(v) for k, v in json_dict.items() if
                    v and self.filter_out_null_values(v)}
        elif isinstance(json_dict, list):
            return [self.filter_out_null_values(v) for v in json_dict if v and self.filter_out_null_values(v)]
        else:
            return json_dict

    def get_30days_ago_timestamp(self):

        # 获取当前时间
        now = datetime.datetime.now()

        # 计算30天前的日期
        date_30_days_ago = now - datetime.timedelta(days=30)

        # 移除时间部分
        date_30_days_ago = date_30_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)

        # 转换为毫秒级时间戳
        timestamp = int(time.mktime(date_30_days_ago.timetuple()) * 1000)
        return timestamp

    def get_servertoken(self):
        # 根据实际情况调整路径
        url = "https://sso.houseinbox.com/authorize/sso/doLogin"

        # 使用提供的用户名和密码
        username = self.config_data['username']
        password = self.config_data['password']
        cook_userNumber = self.config_data['cook_userNumber']
        cook_userid = self.config_data['cook_userid']

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
            url = 'https://sso.doccenter.net/authorize/sso/crossSiteCookie?targetDomain=doccenter.net&userId={}&random={}'.format(
                cook_userid, json_response['random'])
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
                return [json_response['serviceToken'], service_token]
            else:
                print("Error retrieving the URL.")
        else:
            print("请求失败")

    def download_file(self):

        url = "https://houseinbox.com/bshop/order/xaz/hibService2/exportLogisticsStatus"
        headers = {'userid': self.userid,
                   'Servicetoken': self.get_servertoken()[0],
                   'Currentdomain': 'houseinbox.com'}
        payload = {'aquery': {'acreatetimefrom': self.get_30days_ago_timestamp()}}
        # 发送请求
        response = requests.post(url, json=payload, headers=headers)
        # 检查响应状态
        if response.status_code == 200:
            # 将数据读入pandas DataFrame
            try:
                # 使用Python的csv模块进行文件处理
                file_object = io.StringIO(response.content.decode('gb18030'))
                reader = csv.reader(file_object)
                corrected_rows = []
                for row in reader:
                    if len(row) < 16:
                        row += [None] * (16 - len(row))  # Assure each row has at least 16 elements
                    corrected_rows.append([el for el in row[:16]])  # Keep only first 16 elements of each row
                columns = corrected_rows.pop(0)
                # 创建DataFrame, 我们只选择了每个行的前16个元素。
                # 并且通过 columns 参数设定列名
                df = pd.DataFrame(corrected_rows, columns=columns)
                return df
            except Exception as e:
                print(f"鸿渐于陆海运费数据报错，报错信息如下：\n{e}", ['18062351119'])
                return None
        else:
            print(f"鸿渐于陆海运费数据报错，状态码：" + str(response.status_code), ['18062351119'])
            return None

class Freight_Charge:
    def __init__(self):
        yangguang_key = 'f87e1335-63d5-4d68-8d0a-2c41e269d636'
        furong_key = '0e4cf948-7d5d-4993-a6fb-892d9d6a2804'
        xiaowang_key = 'b05001b1-2444-4902-90f7-d58a04ee525e'
        guyue_key = 'f9c9288d-017f-4a63-a112-7ae60d7f94f8'
        self.hongjian_key = 'f7513510-eaf7-446b-946a-a5e8a6dd3d9f'
        self.data_dir = ''
        self.mx_key_list = [yangguang_key,self.hongjian_key]
        self.eu_key_list = [furong_key,guyue_key,self.hongjian_key]
        self.us_key_list = [xiaowang_key,guyue_key,self.hongjian_key]

    def bot_push_text(self, msg, mobile_list=None, key="1c25a6a2-1b6a-489e-96f9-e684296dd672"):
        """发送文本

        Args:
            key (_type_): _description_
            msg (_type_): _description_
        """
        # if mobile_list is None:
        #     mobile_list = ['18062351119']
        webhook_data = {
            "msgtype": "text",
            "text": {
                "content": msg,
                "mentioned_mobile_list": mobile_list
            }
        }

        # 机器人发送
        self.bot_push(key, webhook_data)
        return None

    def bot_push(self, key, data):
        """发送请求结果

        Args:
            key (_type_): _description_
            data (_type_): _description_

        Returns:
            _type_: _description_
        """
        try:
            res = self.robot(key, data)
            print(res)  # 打印请求结果
            print(f'webhook 发出完毕: {res}')
            return res
        except Exception as e:
            print(e)

    def robot(self, key: str, data: Dict[str, Any]) -> str:
        """
        发送企业微信机器人消息

        Args:
            key: 企业微信机器人密钥
            data: 发送的消息内容，具体格式参考企业微信机器人API文档

        Returns:
            发送结果，成功返回'{"errcode":0,"errmsg":"ok"}'，失败返回其他字符串
        """
        webhook = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}"
        headers = {'content-type': 'application/json'}
        r = requests.post(webhook, headers=headers, json=data)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f'发送消息失败，错误信息：{e}')
            return ''
        else:
            print(f'发送消息成功，返回结果：{r.text}')
            return r.text

    def bot_push_markdown(self,report, key="1c25a6a2-1b6a-489e-96f9-e684296dd672"):
        webhook_data = {
            "msgtype": "markdown",
            "markdown": {
                "content": report
            }
        }

        # 调用已有的机器人发送函数
        self.bot_push(key, webhook_data)
    def read_data(self):
        df = pd.read_excel(self.data_dir, sheet_name='Sheet1')
        for index, row in df.iterrows():
            # quote_by = row['报价人']
            origin_port = row['起运港']
            destination_port = row['目的港']
            destination_warehouse = row['仓库']
            shipping_type = row['（CYCY/CYdoor）']
            price = row['报价']
            expiry_date = row['有效期']
            area_classification = row['片区分类']  # 新增的变量

            output_str = f"""
            起运港：\t{origin_port}
            目的港：\t{destination_port}
            目的仓库：\t{destination_warehouse}
            CYCY/CYdoor：\t{shipping_type}
            报价：\t{price}
            有效期：\t{expiry_date}
            """
            if area_classification == '墨西哥':
                for key in self.mx_key_list:
                    self.bot_push_text(output_str,key=key)
            elif area_classification == '美加':
                for key in self.us_key_list:
                    self.bot_push_text(output_str,key=key)
            elif area_classification == '欧洲':
                for key in self.eu_key_list:
                    self.bot_push_text(output_str,key=key)
            else:
                self.bot_push_text(f'有未识别片区值{area_classification}',key=self.hongjian_key)

def main1():
    try:
        df = Abnormal_Withdrawal.download_file

        # 筛选条件
        # 包裹状态（Package Status）为 Shipped
        condition1 = df['包裹状态/Package Status'] == 'Shipped'
        # 物流状态描述（Logistics Status）为空 或 'Shipment information sent to FedEx' 或 'Shipper created a label'
        condition2 = df['物流状态描述/Logistics Status'].isin(
            ['', 'Shipment information sent to FedEx', 'Shipper created a label'])

        # 进行筛选
        df_filtered = df.loc[condition1 & condition2]
        df_filtered.to_csv('海外仓提货异常.csv', index=False, encoding='gb18030')
        return '成功'
    except Exception as e:
        return f'海外仓提货异常表格发生错误，报错信息{e}'

def main2():
    Freight_Charge.read_data()

if __name__ == '__main__':
    main1()