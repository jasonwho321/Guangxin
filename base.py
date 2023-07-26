# coding:utf-8
import json
from urllib.parse import quote_plus

import requests
import base64
import hashlib
from sqlalchemy import create_engine
from typing import Any, Dict
from dateutil.parser import parse

def convert_date(date_string):
    parsed_date = parse(date_string)
    return parsed_date.strftime('%Y-%m-%d')
def get_engine(config_file_path = '/Users/huzhang/PycharmProjects/Guangxin/database/SQLserver.json', db_type='mssql'):
    config_data = read_config(config_file_path)
    server = config_data['server']
    database = config_data['database']
    user = config_data['user']
    password = config_data['password']
    driver_path = config_data['driver_path']

    if db_type.lower() == 'mssql':
        # 创建SQL Server数据库连接字符串
        conn_str = (
            f"Driver={{{driver_path}}};"
            f"Server={server};"
            f"Database={database};"
            f"UID={user};"
            f"PWD={password};"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")
    elif db_type.lower() == 'mysql':
        # 创建MySQL数据库连接字符串
        conn_str = (
            f"DRIVER={{{driver_path}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"OPTION=3;"
        )
        conn_str = f"mysql+pymysql://{user}:{password}@{server}/{database}"
        engine = create_engine(conn_str)
    else:
        raise ValueError('Invalid database type. Only "mssql" and "mysql" are supported.')

    return engine

# 示例

def read_config(config_file_path):
    with open(config_file_path, 'r') as f:
        config_data = json.load(f)
    return config_data
def robot(key: str, data: Dict[str, Any]) -> str:
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


def bot_push(key, data):
    """发送请求结果

    Args:
        key (_type_): _description_
        data (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        res = robot(key, data)
        print(res)  # 打印请求结果
        print(f'webhook 发出完毕: {res}')
        return res
    except Exception as e:
        print(e)


def bot_push_text(msg, mobile_list=None, key="1c25a6a2-1b6a-489e-96f9-e684296dd672"):
    """发送文本

    Args:
        key (_type_): _description_
        msg (_type_): _description_
    """
    if mobile_list is None:
        mobile_list = ['18062351119']
    webhook_data = {
        "msgtype": "text",
        "text": {
            "content": msg,
            "mentioned_mobile_list": mobile_list
        }
    }

    # 机器人发送
    bot_push(key, webhook_data)
    return None


def bot_push_image(key):
    """发送图片

    Args:
        key (_type_): _description_
        msg (_type_): _description_
    """

    # 图片base64码
    with open(r"E:\\OneDrive\图片\WeiXin\mmexport1643682690754.jpg", "rb") as f:
        base64_data = base64.b64encode(f.read())
    # base64.b64decode(base64data)
    # print(base64_data)

    # 图片的md5值
    file = open(r"E:\\OneDrive\图片\WeiXin\mmexport1643682690754.jpg", "rb")
    md = hashlib.md5()
    md.update(file.read())
    res1 = md.hexdigest()
    # print(res1)

    webhook_data = {
        "msgtype": "image",
        "image": {
            "base64": base64_data,
            "md5": res1
        }
    }

    # 机器人发送
    # bot_push(key, webhook_data)
    res = robot(key, webhook_data)
    return None


if __name__ == '__main__':
    get_engine()
    # msg = "程序运行完毕"
    # bot_push_text(msg)
    # bot_push_text("23214d5d-a5af-4be3-b8f2-d8ca154e0b57", msg2, mobile_list2)
    # bot_push_image("23214d5d-a5af-4be3-b8f2-d8ca154e0b57")
