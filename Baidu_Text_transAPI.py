# -*- coding: utf-8 -*-

# This code shows an example of text translation from English to Simplified-Chinese.
# This code runs on Python 2.7.x and Python 3.x.
# You may install `requests` to run this code: pip install requests
# Please refer to `https://api.fanyi.baidu.com/doc/21` for complete api document

import requests
import random
import json
import pandas as pd
from hashlib import md5

# Set your own appid/appkey.
appid = '20220616001249518'
appkey = '7W7h7ETStztZXmahMLAR'

# For list of language codes, please refer to `https://api.fanyi.baidu.com/doc/21`
from_lang = 'auto'
to_lang =  'zh'

endpoint = 'http://api.fanyi.baidu.com'
path = '/api/trans/vip/translate'
url = endpoint + path
query_list = pd.read_csv(r'E:\OneDrive\1.csv',index_col=None,header=None,encoding='GBK')
query_list[0].to_list()

for q in query_list:
    query = '1 CHAIR BROKEN'

    # Generate salt and sign
    def make_md5(s, encoding='utf-8'):
        return md5(s.encode(encoding)).hexdigest()

    salt = random.randint(32768, 65536)
    sign = make_md5(appid + query + str(salt) + appkey)

    # Build request
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'appid': appid, 'q': query, 'from': from_lang, 'to': to_lang, 'salt': salt, 'sign': sign}

    # Send request
    r = requests.post(url, params=payload, headers=headers)
    result = r.json()

    # # Show response
    # print(json.dumps(result, indent=4, ensure_ascii=False))

    dst = result['trans_result'][0]['dst']