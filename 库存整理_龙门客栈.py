import xlwings as xw
import pandas as pd
import datetime
import os

class write(object):
    def __init__(self):
        self.os_path = r'E:/OneDrive/广新/新品等级/库存历史/2022年'
        self.save_path = r'E:\OneDrive\广新\新品等级\2022年1月龙门库存整合表3.xlsx'
        self.path_list = []
        for dir in os.listdir(r'E:\OneDrive\广新\新品等级\库存历史\2022年'):
            self.path_list.append(os.path.join(self.os_path,dir))
        self.colname_list = ['国家',
                           '对应账号SKU',
                           '类别category',
                           'On the Way',
                           'On the Way 2\n(Wayfair-Castle Gate仓)',
                           '仓1',
                           '仓2',
                           '仓3',
                           'Castle Gate仓',
                           'FBA仓',
                           '出口易库存仓1\n(对于US 美国，仓1为【美国库存】,\n对于CA 加拿大，仓1为【加拿大库存】)\n对于EU 欧洲，仓1为【英国库存】)',
                           '出口易库存仓2\n(对于EU 欧洲，仓2为【法国库存】)',
                           '出口易库存仓3\n(对于EU 欧洲，仓3为【德国库存】)',
                           'K2',
                           '天池(FR LVA)',
                           '说剑(US SNA)',
                           '美东一海通（天下）\n(US ATL)',
                           '美东（天运）\n(US PHL)',
                           '美西一海通（达生）\n(US BUR)',
                           '美西（天道）\n(US LSQ)',
                           '圆融汇通（逍遥）\n(CA YTZ)',
                           '德国嘉宏（山木）\n(DE DUS)',
                           '大森林（宗师）\n(FR BVA)',
                           '桑楚\n(US PDK)',
                           '北游\n(US ONO)',
                           '齐物\n(US CCP)',
                           '养生\n(DE NRN)',
                           '人间\n(US GSP)',
                           'RICHMOND HILL',
                           '说疑\n(CA YMX)',
                           '问辩\n(CA YUL)',
                           '三角洲Delta',
                           'FR DOL',
                           'DE FKB',
                           'Houston',
                           'Castle\n(OVERSTOCK)',
                           'CPA\n(OVERSTOCK)',
                           'WKC\n(OVERSTOCK)',
                           'US-OVERSTOCK-CALIFORNIA\n(OVERSTOCK)',
                           '在途+海外可售库存',
                           '海外可售库存',
                           '入库商家',
                           'CBM',
                           'FOB出运港',
                           '工厂'
                           ]

    def open_book(self,app,path):
        date = path[-25:-17]
        date = datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        print(date)
        book = app.books.open(path)
        sht = book.sheets[1]
        df = sht.range((1,1), sht.used_range.shape)
        # 第一行作为标签，后面作为dataframe
        df = pd.DataFrame(df.value[1:], columns=df.value[0])
        outfile_or = df[(df[u'四字机构'] == u'龙门客栈')]
        outfile = pd.DataFrame()
        for l in self.colname_list:
            try:
                outfile[l] = outfile_or[l]
            except:
                outfile[l] = None
                pass
        outfile['date'] = date
        # outfile.date = date
        row1 = outfile.shape[0]
        book.close()
        return outfile,row1

    def main(self):
        app = xw.App(visible=True,add_book=False)
        book1 = app.books.open(self.save_path)
        sht = book1.sheets[0]
        # dataframe_file = pd.DataFrame()
        sht.range((2,1)).value = self.colname_list
        i = 3
        for path in self.path_list:
            dataframe,row = self.open_book(app,path)
            # dataframe_file=pd.concat([dataframe_file,dataframe])
            sht.range((i,1)).options(pd.DataFrame,index=False,header=False).value = dataframe
            i += row
        book1.save()
        book1.close()
        app.quit()


if __name__ == '__main__':
    write().main()