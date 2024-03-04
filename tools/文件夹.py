import os
import csv
import shutil
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import pandas as pd

def get_all_files(path):
    result = []
    for root, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(root, file)
            result.append(full_path)
    return result

def write_to_csv(filepaths, filename='filepaths.csv'):
    with open(filename, 'w', newline='', encoding='gb18030') as f:
        writer = csv.writer(f)
        for path in filepaths:
            writer.writerow([path])

def filter_files(name, filename='filepaths.csv'):
    filtered = []

    with open(filename, 'r', newline='', encoding='gb18030') as f:
        reader = csv.reader(f)
        for row in reader:

            # Condition to exclude paths containing "zgl"
            if "zgl" not in row[0].lower():

                # Check if the specific name is in the file name, not the directory
                if name.lower() in os.path.basename(row[0]).lower():
                    filtered.append(row[0])

    return filtered
def move_files(name, sku, filename='filepaths.csv'):
    filtered = filter_files(name, filename)
    if filtered:
        new_folder = f'Z:\\天空之城\\39F 独立站图片库\\{name} {sku}'
        os.makedirs(new_folder, exist_ok=True)
        for idx, file in enumerate(filtered, 1):
            name_part, extension_part = os.path.splitext(os.path.basename(file))
            new_file_name = f"{name} {sku}({idx}){extension_part}"
            new_file_path = os.path.join(new_folder, new_file_name)
            shutil.move(file, new_file_path)
        return name, "已移动"
    else:
        return name, "没有找到文件"

if __name__ == "__main__":
    df = pd.read_csv(r'C:\Users\Admin\Nutstore\1\我的坚果云\SKU 清单.csv')
    data = zip(df['英文名'].tolist(), df['SKU'].tolist())
    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(lambda pair: move_files(*pair), data), total=len(df)))
    df_results = pd.DataFrame(results, columns=['英文名', '结果'])
    df_results.to_csv('output.csv', index=False)