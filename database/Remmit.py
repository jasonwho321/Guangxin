import csv
import os
import time

import pandas as pd
from guangxin_base import convert_date
import requests
from upload_database import upload_to_sql_server
from scrapy_file.log_in import get_cookies_for_account
payments_header_ca = ['Wayfair Remittance #', 'Payment_Date', 'Invoice #', 'PO #', 'Invoice Date', 'Product Amount',
                      'Wayfair Allowance for Damages/ Defects (4%)', 'Wayfair Early Pay Discount (3%)', 'Shipping',
                      'Other', 'Tax/VAT', 'Payment Amount', 'Business', 'Order Type']
payments_header_us = ['Wayfair Remittance #', 'Payment_Date', 'Invoice #', 'PO #', 'Invoice Date', 'Product Amount',
                      'Wayfair Allowance for Damages/ Defects (4%)', 'Joss & Main Allowance for Damages/ Defects (4%)',
                      'Wayfair Early Pay Discount (3%)', 'Joss & Main Early Pay Discount (3%)', 'Shipping',
                      'Other', 'Tax/VAT', 'Payment Amount', 'Business', 'Order Type']

csv_file_path_us = "/Users/huzhang/Downloads/Payments_Summary.csv"
csv_file_path_ca = "/Users/huzhang/Downloads/Payments_Summary (1).csv"


def download_wf_payment(country):
    # 创建一个文件夹来保存下载的文件
    country_params = {
        'US': {
            'csv_file_path': csv_file_path_us,
            'url_template': 'https://partners.wayfair.com/v/finance/payment/payments_summary/create_oms_csv?voucher_id={voucher_id}'
        },
        'CA': {
            'csv_file_path': csv_file_path_ca,
            'url_template': 'https://partners.wayfair.com/v/finance/payment/payments_summary/create_oms_csv?voucher_id={voucher_id}'
        }
        # 其他国家的参数也可以添加在这里
    }

    # 根据国家选择参数
    params = country_params.get(country, country_params['US']) # 默认选择'US'的参数
    cookies = get_cookies_for_account(country)
    csv_file_path = params['csv_file_path']
    url_template = params['url_template']

    output_folder = f"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对/downloaded_files_"+country
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 读取CSV文件的第一列，获取voucher_ids
    voucher_ids = []
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5',
        'cookie': cookies,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    with open(csv_file_path, "r") as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # 跳过标题行
        for row in reader:
            voucher_ids.append(int(row[0]))


    for voucher_id in voucher_ids:
        url = url_template.format(voucher_id=voucher_id)

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = requests.get(url, headers=headers, stream=True)
                # 如果请求成功，则跳出循环
                break
            except requests.exceptions.SSLError as e:
                print(f"Error occurred: {e}. Retrying in 1 second...")
                retry_count += 1
                time.sleep(1)  # 休息1秒后重试

        # 检查是否超过了最大重试次数
        if retry_count == max_retries:
            print("Max retries exceeded. Unable to fetch the data.")
        else:
            # 检查请求是否成功
            if response.status_code == 200:
                file_name = f"{output_folder}/voucher_{voucher_id}.csv"
                with open(file_name, "wb") as f:
                    f.write(response.content)
                print(f"文件已下载：{file_name}")
            else:
                print(f"无法下载voucher_id为 {voucher_id} 的文件，错误代码：{response.status_code}")

            print("Data fetched successfully.")


def process_csv(input_file,country):
    # 读取CSV文件
    with open(input_file, mode='r') as csvfile:
        data = list(csv.reader(csvfile))

    # 初始化数据列表
    payments = []
    deductions = []
    print(input_file)
    # 查找关键信息
    if data:
        remittance_number = data[0][0].split(': ')[-1].strip()
    else:
        return None,None
    if country == 'CA':
        if len(data[2]) > 1:
            payment_date = data[2][0].split(': ')[-1].strip() + " " + data[2][1].strip()
        else:
            print(
                f"The CSV file '{input_file}' does not have the expected format for the payment date. Skipping this file.")
            return [], []
    else:
        if len(data[2]) == 1:
            payment_date = data[2][0].split(': ')[-1].strip()
        else:
            print(
                f"The CSV file '{input_file}' does not have the expected format for the payment date. Skipping this file.")
            return [], []

    payments_start = None
    deductions_start = None
    for i, row in enumerate(data):
        if row and row[0] == 'Invoice #':
            payments_start = i
            break

    if payments_start is None:
        print(
            f"The CSV file '{input_file}' does not have the expected format for the payments start. Skipping this file.")
        return [], []

    for i, row in enumerate(data):
        if row and row[0] == 'Deduction':
            deductions_start = i
            break

    if deductions_start is None:
        print(
            f"The CSV file '{input_file}' does not have the expected format for the deductions start. Skipping this file.")
        return [], []

    if country == 'CA':
        payments_header = payments_header_ca
    else:
        payments_header = payments_header_us

    # 查找Payments表格的列名在表头中的索引
    col_name_to_idx = {col_name: idx for idx, col_name in enumerate(data[payments_start])}

    for row in data[payments_start + 1:deductions_start - 2]:
        if row:
            # 使用固定的表头初始化一个新的字典并将值设置为0
            payment_entry = {header: 0 for header in payments_header[2:]}

            # 遍历行中的值并将其插入到正确的字典项中
            for col_name, idx in col_name_to_idx.items():
                if col_name in payment_entry:
                    payment_entry[col_name] = row[idx]

            # 将字典转换为列表并将其添加到payments列表中
            payments.append([remittance_number, payment_date] + list(payment_entry.values()))

    # payments.append(['Wayfair Remittance #', 'Payment_Date'] + data[payments_start])
    # for row in data[payments_start+1:deductions_start-2]:
    #     if row:
    #         payments.append([remittance_number, payment_date] + row)

    deductions_header = ['Wayfair Remittance #', 'Payment_Date', 'Deduction ID', 'Deduction Date', 'Deduction Amount', 'Item', 'Item Qty', 'Customer', 'Reason', 'RA#', 'Description']
    deductions.append(deductions_header)

    deduction_entry = {header: '' for header in deductions_header[2:]}

    for row in data[deductions_start:]:
        if row:
            if row[0].startswith('Deduction'):
                if any(deduction_entry.values()):
                    deductions.append([remittance_number, payment_date] + list(deduction_entry.values()))
                    deduction_entry = {header: '' for header in deductions_header[2:]}
                if country == 'CA':
                    deduction_id, deduction_date, deduction_amount = row[1], row[2] + " " + row[3], row[-1] if len(row) > 4 else ''
                else:
                    deduction_id, deduction_date, deduction_amount = row[1], row[2], row[-1] if len(
                        row) > 4 else ''
                deduction_entry['Deduction ID'] = deduction_id
                deduction_entry['Deduction Date'] = deduction_date
                deduction_entry['Deduction Amount'] = deduction_amount
            else:
                for field in deductions_header[5:]:
                    for idx, value in enumerate(row):
                        if field == 'Item' and value.startswith(field):
                            value = row[idx].split(': ')[-1].strip()
                            deduction_entry[field] = value
                        elif field == 'Item Qty' and "Qty" in row[idx]:
                            value = row[idx].split(': ')[-1].strip()
                            deduction_entry[field] = value
                        elif field == 'Description' and row[idx].startswith("Desc"):
                            value = ' '.join(row[idx:]).split(': ')[-1].strip().replace('<br>', '')
                            deduction_entry[field] = value
                        elif value.startswith(field):
                            value = row[idx].split(': ')[-1].strip()
                            deduction_entry[field] = value

    if any(deduction_entry.values()):
        deductions.append([remittance_number, payment_date] + list(deduction_entry.values()))

    return payments, deductions


def read_and_merge_csv_files(file_folder):
    # 读取CSV文件
    deductions_ca = pd.read_csv(file_folder+'/deductions_CA.csv')
    deductions_us = pd.read_csv(file_folder+'/deductions_US.csv')
    payments_ca = pd.read_csv(file_folder+'/payments_CA.csv')
    payments_us = pd.read_csv(file_folder+'/payments_US.csv')
    # 添加国家列
    deductions_ca['Country'] = 'CA'
    deductions_us['Country'] = 'US'
    payments_ca['Country'] = 'CA'
    payments_us['Country'] = 'US'
    # 合并美国和加拿大的扣除数据
    deductions = pd.concat([deductions_ca, deductions_us], ignore_index=True)

    # 为加拿大数据添加两列并将值设置为0
    payments_ca['Joss & Main Allowance for Damages/ Defects (4%)'] = 0
    payments_ca['Joss & Main Early Pay Discount (3%)'] = 0

    # 按列名重新排序以匹配美国数据的列顺序
    payments_ca = payments_ca[payments_us.columns]

    # 合并美国和加拿大的支付数据
    payments = pd.concat([payments_ca, payments_us], ignore_index=True)

    # 清洗日期格式
    payments['Payment_Date'] = payments['Payment_Date'].apply(convert_date)
    payments['Invoice Date'] = payments['Invoice Date'].apply(convert_date)
    deductions['Payment_Date'] = deductions['Payment_Date'].apply(convert_date)
    deductions['Item Qty'] = deductions['Item Qty'].str.replace('"', '')  # 移除引号
    deductions['Item Qty'] = deductions['Item Qty'].astype(float)  # 转换为浮点数
    deductions['Deduction Amount'] = deductions['Deduction Amount'].astype(str).str.replace('"', '').astype(float)
    return payments, deductions


def main(folder_path,country):

    deductions_header = ['Wayfair Remittance #', 'Payment_Date', 'Deduction ID', 'Deduction Date', 'Deduction Amount', 'Item', 'Item Qty', 'Customer', 'Reason', 'RA#', 'Description']

    # 初始化总列表并添加表头
    if country == 'CA':
        all_payments = [payments_header_ca]
    else:
        all_payments = [payments_header_us]
    all_deductions = [deductions_header]

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            input_file = os.path.join(folder_path, filename)
            payments, deductions = process_csv(input_file,country)
            if payments or deductions:
                # 将结果添加到总列表中
                all_payments.extend(payments[1:])  # 从第二行开始添加，跳过表头
                all_deductions.extend(deductions[1:])

    # 输出Payments和Deductions到CSV文件

    with open(f'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对/payments_{country}.csv', mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(all_payments)

    with open(f'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对/deductions_{country}.csv', mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(all_deductions)


if __name__ == '__main__':
    country_list = ['US','CA']
    for country in country_list:
        download_wf_payment(country)
        folder_path = f"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对/downloaded_files_{country}/"
        main(folder_path,country)

    payments, deductions = read_and_merge_csv_files(file_folder='/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对')
    payments.to_csv('payments.csv',index_label=False)
    deductions.to_csv('deductions.csv',index_label=False)

    table_name_deductions = 'WF_Deductions'
    table_name_payments = 'WF_Payments'
    schema = "Finance"
    timestamp_column = 'Payment_Date'

    upload_to_sql_server(payments,table_name=table_name_payments,schema=schema,timestamp_column=timestamp_column,if_exists='append_no_duplicates')
    upload_to_sql_server(deductions,table_name=table_name_deductions,schema=schema,timestamp_column=timestamp_column,if_exists='append_no_duplicates')