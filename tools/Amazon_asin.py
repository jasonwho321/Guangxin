import pandas as pd
import re

data = pd.read_excel('/Users/huzhang/Desktop/Amazon_SKU.xlsx')

def extract_code(url):
    if pd.isna(url) or not isinstance(url, str):
        return None
    match = re.search(r'/(B[0-9A-Z]{9})', url)
    if match:
        return match.group(1)
    return None

extracted_data = []
for index, row in data.iterrows():
    valid_entries = {'HIB SKU': row['HIB SKU']}
    amazon_code = extract_code(row['Amazon链接'])
    competitor_code = extract_code(row['Amazon竞品链接'])

    if amazon_code:
        valid_entries['Amazon链接'] = amazon_code
    if competitor_code:
        valid_entries['Amazon竞品链接'] = competitor_code

    if amazon_code or competitor_code:
        extracted_data.append(valid_entries)

extracted_df = pd.DataFrame(extracted_data)
extracted_df.to_excel('/Users/huzhang/Desktop/extracted_data.xlsx', index=False)
