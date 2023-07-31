# coding:utf-8
import json
from urllib.parse import quote_plus

import requests
import base64
import hashlib
from sqlalchemy import create_engine
from typing import Any, Dict
from dateutil.parser import parse
from file_path_config import paths
import os
import platform
import subprocess


def get_windows_version():
    try:
        output = subprocess.check_output("wmic os get caption", shell=True).decode('gb18030')
        return output.split('\n')[1].strip()  # 第二行是操作系统名称
    except Exception as e:
        print(f'Error when getting windows version: {e}')
        return None




def get_system_path(path_key, full_path=None):
    """
    根据提供的键值和全路径获取在当前系统中的完整路径。

    参数:
    path_key -- 字典中路径的键值，可选项有 'webdriver_executable_path', 'binary_location', 'SQLserver', 'airy_hib_account', 'password_file'
    full_path -- 文件在坚果云中的全路径，如果文件不在坚果云中，可以设置为 None，默认值为 None

    返回值:
    在当前系统中的文件全路径
    """
    system = platform.system()

    # 根据系统获取用户主目录和坚果云文件夹名称
    home = None
    nutstore_folder = None
    specified_path = None
    if system == 'Windows':
        windows_version = get_windows_version()
        if 'Windows 10' in windows_version:
            home = paths[system]['Windows 10']['home']
            nutstore_folder = paths[system]['Windows 10']['nutstore_folder']
            specified_path = paths[system]['Windows 10'].get(path_key, None)
        elif 'Server 2019' in windows_version:
            home = paths[system]['Server 2019']['home']
            nutstore_folder = paths[system]['Server 2019']['nutstore_folder']
            specified_path = paths[system]['Server 2019'].get(path_key, None)
    elif system in ['Linux', 'Darwin']:  # Linux or MacOS
        home = paths[system]['home']
        nutstore_folder = paths[system]['nutstore_folder']
        specified_path = paths[system].get(path_key, None)

    # 如果full_path为None，返回从字典中取得的路径
    if full_path is None:
        return specified_path

    # 找到坚果云文件夹在完整路径中的位置
    path_parts = full_path.split('/')
    nutstore_index = path_parts.index(nutstore_folder) if nutstore_folder in path_parts else -1

    if nutstore_index == -1:
        print(f'Could not find "{nutstore_folder}" in the provided path.')
        return None

    # 构造新的路径
    relative_path = os.path.join(*path_parts[nutstore_index+1:])
    full_system_path = os.path.join(home, relative_path)

    # 根据系统路径分隔符来替换路径
    full_system_path = full_system_path.replace('/', os.sep)

    return full_system_path


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
    print(get_system_path(None,'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/BI小组文件存档/账号信息.xlsx'))
    print(get_system_path('webdriver_executable_path'))
    # msg = "程序运行完毕"
    # bot_push_text(msg)
    # bot_push_text("23214d5d-a5af-4be3-b8f2-d8ca154e0b57", msg2, mobile_list2)
    # bot_push_image("23214d5d-a5af-4be3-b8f2-d8ca154e0b57")
