# coding:utf-8
import pandas as pd
import xlwings as xw
from datetime import datetime
import requests
import base64
import hashlib

api_key = "23214d5d-a5af-4be3-b8f2-d8ca154e0b57"
report_dir = r'C:\Users\Admin\Downloads\晓望日报表2022.xlsx'
contact_dir = r'C:\Users\Admin\Nutstore\1\我的坚果云\日报表通知名单.xlsx'
today = datetime.today().strftime("%Y-%m-%d")
today1 = datetime.today().strftime("%m月%d日")


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


def bot_push_text(key, msg, mobile_list):
    """发送文本

    Args:
        key (_type_): _description_
        msg (_type_): _description_
    """
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


def main():
    app = xw.App(visible=True, add_book=False)
    wb = app.books.open(report_dir)
    sht = wb.sheets[0]

    nrow = sht.used_range.last_cell.row
    ncol = sht.used_range.last_cell.column

    date_row = sht.range((3, 1), (3, ncol)).value
    department_row = sht.range((1, 1), (nrow, 1)).value

    # date_colomn = date_row.index(today)
    date_colomn = date_row.index(today) + 1
    department_row = department_row.index('太华天街') + 1

    index_key = sht.range((department_row, 5), (nrow, 5)).value
    index_value_old = sht.range(
        (department_row, date_colomn), (nrow, date_colomn)).value
    index_value = []
    for i in index_value_old:
        item = 0 if i is None else i
        index_value.append(item)
    index_dict = dict(zip(index_key, index_value))
    orders = format(
        int(index_dict['Total Orders 电商日出单总量'] + index_dict['Total Orders 传统日出单总量']), ',')
    total_amount = format(
        (float(
            index_dict['Total Amount 电商日总营业额 （USD)']) +
            index_dict['Total Amount 传统日总营业额 （USD)']),
        ',')
    profit = format(float(index_dict['Profit 利润 (USD)']), ',')
    stock = format(
        int(index_dict['Oversea Stock & On the Way Stock Qty 海外在仓在途库存数量']), ',')
    turnover = format(int(index_dict['Turnover Days 周转天数']), ',')
    daily_list = [
        today1,
        str(orders),
        str(total_amount),
        str(profit),
        str(stock),
        str(turnover)]
    wb.close()
    app.quit()
    return daily_list


def contact():
    df = pd.read_excel(contact_dir)
    result = df.loc[df['日期'] == today]
    number = result['手机号']
    name = result['负责人']
    number = str(number.loc[0])
    name = str(name.loc[0])
    return number, name
    # print('结果为：\n{}\n类型为：{}'.format(result,type(result)))


def run():
    number, name= contact()
    daily_list = main()
    msg1 = "日期：{0[0]}\n晓望集群单量：{0[1]}\n晓望集群营业额：{0[2]}\n晓望集群利润：{0[3]}\n晓望集群库存量：{0[4]}\n晓望集群库存周转天数：{0[5]}".format(
        daily_list)
    msg2 = '请 {} 发送日报截图和简报数据到微信群'.format(name)
    mobile_list1 = []
    mobile_list2 = [number]
    bot_push_text(api_key, msg1, mobile_list1)
    bot_push_text(api_key, msg2, mobile_list2)


if __name__ == '__main__':
    run()
