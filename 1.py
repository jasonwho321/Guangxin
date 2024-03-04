import pandas as pd
import re

def convert_to_days(time_str):
    match = re.match(r'(\d+)\s*(.*)', time_str)
    if match:
        num = int(match.group(1))
        unit = match.group(2)
        if unit == "年前":
            return num * 365
        elif unit == "个月前":
            return num * 30
        elif unit == "周前":
            return num * 7
        elif unit == "天前":
            return num
        else:
            return None
    else:
        return None

df = pd.read_csv('youtube_data.csv', encoding='gb18030')
df['上传时间'] = df['上传时间'].apply(convert_to_days)

print(df['上传时间'])

df.to_csv('youtube_data_modified.csv', encoding='gb18030', index=False)