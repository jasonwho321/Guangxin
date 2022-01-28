import os
from imbox import Imbox
import datetime
import shutil

imbox = Imbox('imap-n.global-mail.cn',
              username='john.hu@39f.net',
              password='Hu951119!',
              ssl=True)

messages_from = imbox.messages(folder='datacenter',date__gt=datetime.date(2022, 1, 5))

for uid, message in messages_from:
    attachments = message.attachments
    print(attachments)
    for attachment in attachments:
        print(attachment)
        with open(attachment['filename'], 'wb') as f:
            f.write(attachment['content'].getvalue())
            f.close()
            if attachment['filename'].endswith(('.xlsx')) and '全球库存统计及补' in attachment['filename']:
                try:
                    shutil.move(attachment['filename'],'E://OneDrive//广新//新品等级//库存历史 2022//')
                except:
                    os.remove('E://OneDrive//广新//新品等级//库存历史 2021//'+attachment['filename'])
                    shutil.move(attachment['filename'], 'E://OneDrive//广新//新品等级//库存历史 2022//')
                    pass
            else:

                pass
        os.remove('./'+attachment['filename'])
