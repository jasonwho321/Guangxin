import requests
from bs4 import BeautifulSoup
import jieba.posseg as pseg
from pandas.core.frame import DataFrame

total_title_list = []
for i in range(1,34):
    print(i)
    url = 'https://gd.huatu.com/gxlx/kaoshi/'+str(i)+'.html'
    session = requests.session()
    req_header = {
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
    }

    response = session.get(url,headers=req_header)
    response.encoding = 'gbk'
    soup = BeautifulSoup(response.text)
    h3_list = soup.find_all(name='h3')
    title_link_dic = {}
    for h in h3_list:
        title_link = h.a.attrs
        link = title_link['href']
        title = title_link['title']
        title_link_dic[title] = link
    print(title_link_dic)

    for t in title_link_dic:
        words = pseg.cut(t)
        title_list = [t]
        for word, flag in words:
            if flag == 'm' and word not in ['2021','2020','2019','2018','2017','2016','年','月']:
                title_list.append(word)
            if flag == 'ns':
                title_list.append(word)
        total_title_list.append(title_list)

    data = DataFrame(total_title_list)
    data.rename(columns={0:'title'})
    data.to_csv(r'E:\OneDrive\数据分析\华图公选.csv',encoding='utf-8-sig')