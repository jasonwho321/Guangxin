from imbox import Imbox
import datetime
import shutil

imbox = Imbox('imap-n.global-mail.cn',
              username='john.hu@39f.net',
              password='Hu951119!',
              ssl=True)

messages_from = imbox.messages(folder='datacenter',date__gt=datetime.date(2021, 10, 13))

for uid, message in messages_from:
    attachments = message.attachments
    print(attachments)
    for attachment in attachments:
        print(attachment)
        with open(attachment['filename'], 'wb') as f:
            f.write(attachment['content'].getvalue())
            f.close()
            if attachment['filename'].endswith(('.xlsx')) and '全球库存统计及补' in attachment['filename']:
                shutil.move(attachment['filename'],'E://OneDrive//广新//新品等级//库存历史 2021//')
            else:
                pass
