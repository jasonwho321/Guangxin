import json
import glob
import requests
from base import bot_push_text
from scrapy.log_in import *
import os
from datetime import datetime, timedelta
from upload_database import upload_cg_ful
# 获取当前日期
now = datetime.now()

# 获取两周前的日期
createDateRange_from = now - timedelta(weeks=2)
# 将日期格式化为字符串
createDateRange_from = createDateRange_from.strftime("%Y-%m-%d")

# 获取当前日期（数据下载结束日期）
createDateRange_to = now.strftime("%Y-%m-%d")

id_ca = 35722
id_us = 44345

ful_folder_path_ca = get_system_path(None,"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Fulfillment_CA")
tran_folder_path_ca = get_system_path(None,"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Transportation_CA")
ful_folder_path_us = get_system_path(None,"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Fulfillment_US")
tran_folder_path_us = get_system_path(None,"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Transportation_US")

first_button_xpath = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div/button"
second_button_xpath_ca = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div[2]/div/ul/li[2]/button"
second_button_xpath_us = "/html/body/div[2]/div/div/div/div/div[2]/div[1]/div/div/div/div[2]/div[1]/div[2]/div/ul/li[1]/button"

def get_download_list(country,cookies):
    if country == 'US':
        partner_id = id_us
    else:
        partner_id = id_ca

    headers = {
        'accept': 'application/json',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5',
        'cookie': cookies,
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.50'
    }

    payload = {
        "arInvoiceFetchInput": {
            "recipients": [
                {"type": "SUPPLIER", "id": partner_id}
            ],
            "revenueSourceTypes": ["CASTLEGATE", "SPONSORED_PRODUCTS", "MAAS_MEDIA"],
            "statuses": ["APPROVED", "SENT_TO_RECIPIENT", "PENDING_PAYMENT", "FULLY_PAID", "PARTIALLY_PAID"],
            "createDateRange": {"from": createDateRange_from, "to": createDateRange_to}
        },
        "spoId": ""
    }

    url = "https://partners.wayfair.com/a/finance/invoice/payables_summary/search_invoices"
    response = requests.post(url, headers=headers, stream=True, data=json.dumps(payload))

    if response.status_code == 200:
        json_response = response.json()
        return json_response, headers
    else:
        bot_push_text('CG费用账单清单获取失败')


def download_file(country,cookies):
    json_response, headers = get_download_list(country,cookies)
    print(json_response)
    if country == 'US':
        transportation_foldername = get_system_path(None,'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Transportation_US')
        fulfillment_foldername = get_system_path(None,'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Fulfillment_US')
    else:
        transportation_foldername = get_system_path(None,'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Transportation_CA')
        fulfillment_foldername = get_system_path(None,'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/CG_Fulfillment_CA')

    # 创建存储文件的文件夹
    os.makedirs(transportation_foldername, exist_ok=True)
    os.makedirs(fulfillment_foldername, exist_ok=True)

    # 假设您的JSON数据存储在一个名为json_data的字典中
    json_data = json_response['invoices']

    # 创建一个空的DataFrame来存储需要的数据
    df = pd.DataFrame(columns=['createDate', 'invoiceId', 'invoiceAmount'])
    df_mer = pd.DataFrame(columns=['createDate', 'invoiceId', 'invoiceAmount'])

    # 检查每个数据点的revenueSource，并执行相应的操作
    for data in json_data:
        if data['revenueSource'] == 'SPONSORED_PRODUCTS':
            new_row = pd.DataFrame([{
                'createDate': data['createDate'].split(' ')[0],
                'invoiceId': data['invoiceId'],
                'invoiceAmount': data['invoiceAmount']
            }])
            df = pd.concat([df, new_row], ignore_index=True)
        elif data['revenueSource'] == 'TRANSPORTATION':
            url = f"https://partners.wayfair.com/v/finance/invoice/payables_summary/generate_document?invoice_id={str(data['invoiceId'])}&is_csv=1"
            r = requests.get(url, headers=headers)
            with open(f'{transportation_foldername}/{data["invoiceId"]}.csv', 'wb') as f:
                f.write(r.content)
        elif data['revenueSource'] == 'CASTLEGATE':
            url = f"https://partners.wayfair.com/v/finance/invoice/payables_summary/generate_document?invoice_id={data['invoiceId']}&is_csv=1"
            r = requests.get(url, headers=headers)
            with open(f'{fulfillment_foldername}/{data["invoiceId"]}.csv', 'wb') as f:
                f.write(r.content)
        elif data['revenueSource'] == 'MAAS_MEDIA':
            url = "https://partners.wayfair.com/a/finance/invoice/payables_summary/search_invoice_details"
            payload = {"invoiceId": data['invoiceId']}
            r = requests.post(url, json=payload, headers=headers)
            response_data = r.json()
            if 'invoice' in response_data and 'details' in response_data['invoice']:
                for detail in response_data['invoice']['details']:
                    new_row = pd.DataFrame([{
                        'createDate': data['createDate'].split(' ')[0],
                        'invoiceId': data['invoiceId'],
                        'invoiceAmount': data['invoiceAmount'],
                        **detail
                    }])
                    df_mer = pd.concat([df_mer, new_row], ignore_index=True)

    # 保存数据帧为csv
    df.to_csv(get_system_path(None,'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/Media.csv'), index=False)
    df_mer.to_csv(get_system_path(None,'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/水单核对/Merchandising.csv'), index=False)


def concat_csv_to_csv(folder_path, filename):
    # 获取所有CSV文件
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))

    # 初始化空的DataFrame列表
    df_list = []

    # 遍历并读取所有CSV文件，然后将它们添加到列表中
    for file in all_files:
        df = pd.read_csv(file, index_col=None, header=0, low_memory=False)
        df_list.append(df)

    # 将所有数据框架连接到一个数据框架
    concatenated_df = pd.concat(df_list, axis=0, ignore_index=True)

    # 创建保存的csv文件路径
    csv_file_path = os.path.join(os.path.dirname(folder_path), filename + ".csv")

    # 将合并后的数据框架保存为csv文件
    concatenated_df.to_csv(csv_file_path, index=False)

    print(f"Saved combined data to {csv_file_path}")


def process_data(driver, wait, country, tran_folder_path, ful_folder_path, second_button_xpath):
    cookies = get_cookies(driver)
    download_file(country, cookies)
    concat_csv_to_csv(ful_folder_path, 'CG_Fulfillment_' + country)
    concat_csv_to_csv(tran_folder_path, 'CG_Transportation_' + country)
    get_and_click_button(driver, wait, first_button_xpath)
    wait.until(EC.visibility_of_element_located((By.XPATH, second_button_xpath)))
    get_and_click_button(driver, wait, second_button_xpath)


def main():
    driver, button_text, wait = start()

    if button_text == "CAN_39FInc.":
        process_data(driver, wait, 'CA', tran_folder_path_ca, ful_folder_path_ca, second_button_xpath_ca)
    elif button_text == "39fInc.":
        process_data(driver, wait, 'US', tran_folder_path_us, ful_folder_path_us, second_button_xpath_us)

    time.sleep(20)
    wait.until(EC.visibility_of_element_located((By.XPATH, button_text_xpath)))
    button_text = driver.find_element(By.XPATH, button_text_xpath).text
    if button_text == "CAN_39FInc.":
        process_data(driver, wait, 'CA', tran_folder_path_ca, ful_folder_path_ca, second_button_xpath_ca)
    elif button_text == "39fInc.":
        process_data(driver, wait, 'US', tran_folder_path_us, ful_folder_path_us, second_button_xpath_us)

    driver.quit()
    upload_cg_ful()
if __name__ == '__main__':
    main()