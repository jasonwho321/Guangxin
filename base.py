# coding:utf-8
import os
from apscheduler.schedulers.blocking import BlockingScheduler
import requests
import json
import base64
import hashlib

def robot(key, data):
    """_summary_

    Args:
        key (_type_): _description_
        data (_type_): _description_
    """
    webhook = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}"
    # 请求头
    headers = {'content-type': 'application/json'}
    r = requests.post(webhook, headers=headers, json=data)
    r.encoding = 'utf-8'
    print(f'执行内容:{data}, 参数:{r.text}')
    print(f'webhook 发送结果:{r.text}')
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
        "text":{
            "content": msg,
            "mentioned_mobile_list":mobile_list
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
    with open(r"E:\\OneDrive\图片\WeiXin\mmexport1643682690754.jpg","rb") as f:
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
def wfs():
    os.system("python Wayfair_Scrapy.py")
if __name__ == '__main__':
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(wfs, 'cron', hour=0, minute=0)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


    # bot_push_text("23214d5d-a5af-4be3-b8f2-d8ca154e0b57", msg2, mobile_list2)
    # bot_push_image("23214d5d-a5af-4be3-b8f2-d8ca154e0b57")