import os
from imbox import Imbox
import datetime
import shutil


class Mail_get:
    def __init__(self):
        self.location = 'D://Shadowbot//'
        self.username = 'john.hu@39f.net'
        self.password = 'Hu951119!'
        self.imap = 'imap-n.global-mail.cn'

    def dayli_run(self, folder='DataSource'):
        # 按需添加
        stock_age_index = 0
        daily_report_index = 0
        stock_index = 0
        with Imbox(self.imap,
                   username=self.username,
                   password=self.password,
                   ssl=True) as imbox:

            # messages_from = imbox.messages(
            #     folder=folder,
            #     date__gt=datetime.date.today())
            # 测试替换行
            messages_from = imbox.messages(folder=folder,date__gt=datetime.date(2022, 8, 4))

            print(messages_from)
            for uid, message in messages_from:
                attachments = message.attachments
                for attachment in attachments:
                    # 按需添加
                    # stock_age_index += self.stock_age(attachment)
                    # daily_report_index += self.daily_report(attachment)
                    stock_index += self.stock(attachment)

        # 按需添加
        result_dict = {
            # 'stock_age': stock_age_index,
            # 'daily_report': daily_report_index,
            'stock': stock_index}

        return result_dict
        pass

    def weekly_run(self, folder='tableau', date=datetime.date.today()):
        # 按需添加

        weekly_report_index = 0

        with Imbox(self.imap,
                   username=self.username,
                   password=self.password,
                   ssl=True) as imbox:

            messages_from = imbox.messages(
                folder=folder,
                date__gt=date)
            # 测试替换行
            # messages_from = imbox.messages(folder='datacenter',date__gt=datetime.date(2022, 4, 21))

            print(messages_from)
            for uid, message in messages_from:
                attachments = message.attachments
                for attachment in attachments:
                    # 按需添加
                    weekly_report_index += self.week_report(attachment)

        # 按需添加
        result_dict = {
            'week_report': self.week_report}

        return result_dict
        pass

    def stock_age(self, attachment):
        if '库龄' in attachment['filename']:
            with open(attachment['filename'], 'wb') as f:
                f.write(attachment['content'].getvalue())
                f.close()
                try:
                    shutil.move(
                        attachment['filename'],
                        self.location + "stock_age.xlsx")
                except BaseException:
                    os.remove(self.location + "stock_age.xlsx")
                    shutil.move(
                        attachment['filename'],
                        self.location + "stock_age.xlsx")
                    pass
            return 1
        else:
            return 0
            pass

    def daily_report(self, attachment):
        if '日报表' in attachment['filename']:
            with open(attachment['filename'], 'wb') as f:
                f.write(attachment['content'].getvalue())
                f.close()
                try:
                    shutil.move(
                        attachment['filename'],
                        self.location + "晓望日报表//日报表.xlsx")
                except BaseException:
                    os.remove(self.location + "晓望日报表//日报表.xlsx")
                    shutil.move(
                        attachment['filename'],
                        self.location + "晓望日报表//日报表.xlsx")
            return 1
        else:
            return 0
            pass

    def stock(self, attachment):
        if '补货计划表' in attachment['filename']:
            with open(attachment['filename'], 'wb') as f:
                f.write(attachment['content'].getvalue())
                f.close()
                try:
                    shutil.move(
                        attachment['filename'],
                        self.location + attachment['filename'])
                except BaseException:
                    os.remove(self.location + attachment['filename'])
                    shutil.move(
                        attachment['filename'],
                        self.location +attachment['filename'])
            return 1
        else:
            return 0
            pass

    def week_report(self, attachment):
        if '周报' in attachment['filename']:
            with open(attachment['filename'], 'wb') as f:
                f.write(attachment['content'].getvalue())
                f.close()
                try:
                    shutil.move(
                        attachment['filename'],
                        self.location + "周报.pdf")
                except BaseException:
                    os.remove(self.location + "周报.pdf")
                    shutil.move(
                        attachment['filename'],
                        self.location + "周报.pdf")
            return 1
        else:
            return 0
            pass


if __name__ == '__main__':
    main = Mail_get()
    main.dayli_run()
