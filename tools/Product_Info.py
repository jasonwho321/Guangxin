import requests
import json
import re
import pandas as pd
from bs4 import BeautifulSoup
import traceback
from config import proxyip
def retry_request(func, *args, **kwargs):
    max_retries = 3
    for i in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error occurred. Retry {i+1}/{max_retries}. Traceback: {traceback.format_exc()}")
            continue
    raise Exception('Max retries exceeded')

def extract_sku(content):
    pattern = r'sku":"(\w+)"'
    match = re.search(pattern, content)
    if match:
        return match.group(1)
    else:
        return None

def extract_name(content):
    # First find the start and end of the script tag
    start = content.find('<script type="application/ld+json">')
    if start == -1:
        return None
    end = content.find('</script>', start)
    # Then extract and parse the JSON-LD
    json_ld = content[start+len('<script type="application/ld+json">'):end]
    data = json.loads(json_ld)
    # And finally extract the name
    return data.get('name')

def extract_first_url(string):
    pattern = r"(https?://[^\s;]+)"
    match = re.search(pattern, string)
    if match:
        url = match.group(1)
        if url.endswith(">"):
            url = url[:-1]
        return url
    else:
        return None

def get_response(url, headers=None, proxies=None):
    if headers is None:
        headers = load_cookies()
        headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Ed..."
        })
    response = requests.get(url, proxies=proxies, headers=headers)
    return response


def get_first_image(url, headers=None, proxy=None):
    proxies = {
        'http': proxy,
        'https': proxy,
    } if proxy else None
    response = retry_request(get_response, url, headers, proxies)

    if "google-analytics" in response.headers['Link']:
        headers = update_cookies(headers, response.headers)
        save_cookies(headers)
        response = retry_request(requests.get, url, proxies=proxies, headers=headers)

    return extract_first_url(response.headers['Link']),response

def update_cookies(headers, response_headers):
    if headers is None:
        headers = {}
    if 'Cookie' not in headers:
        headers['Cookie'] = ''
    else:
        # 如果已有cookies，则添加分号
        headers['Cookie'] += '; '
    set_cookies = response_headers.get('Set-Cookie', None)
    if set_cookies:
        cookies = set_cookies.split(", ")
        for cookie in cookies:
            name = cookie.split(";")[0]
            # 清洗cookie，移除其中的时间戳部分
            cleaned_name = re.sub(r'; \d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2} GMT', '', name)
            headers['Cookie'] += cleaned_name
    return headers

def save_cookies(headers, filename="cookies.json"):
    with open(filename, "w") as file:
        json.dump({"Cookie": headers['Cookie']}, file)

def load_cookies(filename="cookies.json"):
    try:
        with open(filename, "r") as file:
            cookies = json.load(file)
            # 清洗cookie，移除其中的时间戳部分
            cookies['Cookie'] = re.sub(r'; \d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2} GMT', '', cookies['Cookie'])
    except FileNotFoundError:
        cookies = {"Cookie": ""}
    return cookies

def get_module_info(sku):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1462.54',
        'use-web-hash': 'true'
    }

    proxies = {
        'http': proxyip,
        'https': proxyip,
    }
    link = 'https://www.wayfair.com/graphql'
    sp = retry_request(requests.post,
        link,
        json={"variables":{"sku":sku,"includeUnapprovedModules":False}},
        params={"hash":"b6bd2aee3912f70b28a6ad18d4a9fca4#59effa6f1e60e3fb4275a0ef55f43565"},
        headers=headers,proxies=proxies)


    try:
        content = sp.json()
        modules = content['data']['product']['waymore']['modules']
        total_module_count = len(modules)
        related_video_module_count = sum(1 for module in modules if module['isRelatedVideosModule'] is not None)
        return {'waymore_count': total_module_count, 'waymore_video': related_video_module_count}
    except Exception as e:
        return {'waymore_count': 0, 'waymore_video': 0}

def extract_info_from_url(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.41',
        'use-web-hash': 'true',
        'x-parent-txid':'I+F9OmSBul+v/75eMblyAg=='
    }
    first_image_url,con = get_first_image(url,headers,proxy=proxyip)
    soup = BeautifulSoup(con.content, 'html.parser')
    description_text = soup.find('div', class_='RomanceCopy-text').get_text()

    # 查找"h4"标签，并检查其文本是否为"Features"
    features_div = None
    for h4 in soup.find_all('h4'):
        if h4.get_text(strip=True) == 'Features':
            features_div = h4.parent
            break
    # 如果找到了对应的div，那么提取其中的li元素
    if features_div:
        li_elements = features_div.find_all('li', class_='BulletList-listItem')
        features = '; '.join(li.get_text(strip=True) for li in li_elements)
    else:
        features = '-'

    sku = extract_sku(con.content.decode('utf-8'))
    name = extract_name(con.content.decode('utf-8'))
    waymore_info = get_module_info(sku)

    return sku, name, first_image_url, description_text, features, waymore_info

def main():
    # 读取Excel文件
    df = pd.read_excel("/app/data/89古月弓长/链接.xlsx")

    # 初始化一个空的DataFrame来储存结果
    result = pd.DataFrame(columns=["WF_sku", "标题", "Vendor", "链接", "首图", "39F_SKU", "Description", "Features", "waymore数量", "Waymore视频数量"])

    failed_urls = []
    for index, row in df.iterrows():
        url = row['链接']
        vendor = row['vendor']
        try:
            sku, name, first_image_url, description_text, features, waymore_info = retry_request(extract_info_from_url, url)

            result_temp = pd.DataFrame({
                "WF_sku": [sku],
                "Country":"CA" if ".ca" in url else "US",
                "标题": [name],
                "Vendor": [vendor],
                "链接": [url],
                "首图": [first_image_url],
                "39F_SKU": [row['39F SKU']],
                "Description": [description_text],
                "Features": [features],
                "waymore数量": [waymore_info['waymore_count']],
                "Waymore视频数量": [waymore_info['waymore_video']]
            })
            result = pd.concat([result,result_temp],ignore_index=True)
        except Exception as e:
            print(f'Failed to get data from {url}: {e}')
            print(f"Error occurred. Traceback: {traceback.format_exc()}")
            failed_urls.append(url)
            result_temp = pd.DataFrame({"WF_sku": ['-'],
                                        "标题": ['-'],
                                        "Vendor": [vendor],
                                        "链接": [url],
                                        "首图": ['-'],
                                        "39F_SKU": [row['39F SKU']],
                                        "Description": ['-'],
                                        "Features": ['-'],
                                        "waymore数量": ['-'],
                                        "Waymore视频数量": ['-']})
            result = pd.concat([result, result_temp], ignore_index=True)
        print(f"Processed {index+1}/{len(df)}")

    # 将结果保存到Excel文件中
    result.to_excel("/app/data/89古月弓长/产品信息表.xlsx", index=False)

if __name__ == '__main__':
    main()
