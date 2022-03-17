import os
from imbox import Imbox
import datetime
import shutil

imbox = Imbox('imap-n.global-mail.cn',
              username='john.hu@39f.net',
              password='Hu951119!',
              ssl=True)

messages_from = imbox.messages(folder='datacenter',date__gt=datetime.date(2022,3,3))
# messages_from = imbox.messages(folder='datacenter',date__gt=datetime.date.today())

for uid, message in messages_from:
    attachments = message.attachments
    print(attachments)
    for attachment in attachments:
        print(attachment)
        with open(attachment['filename'], 'wb') as f:
            f.write(attachment['content'].getvalue())
            f.close()
            if attachment['filename'].endswith(('.xlsx')) and '晓望日报表' in attachment['filename']:
                try:
                    shutil.move(attachment['filename'],'E://OneDrive//广新//晓望日报表//')
                except:
                    os.remove('E://OneDrive//广新//晓望日报表//'+attachment['filename'])
                    shutil.move(attachment['filename'], 'E://OneDrive//广新//晓望日报表//')
                    pass
            else:
                os.remove('./' + attachment['filename'])
                pass
