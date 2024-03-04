import pandas as pd

def rename_columns(df):
    """
    Function to rename columns according to the specified rules:
    - Columns ending with '-1' are renamed to '第X周零售均价'.
    - Columns with only a number are renamed to '第X周销量'.
    - Columns ending with '-2' are renamed to '第X周系数'.
    - Column 'product_name' is renamed to '品名'.
    """
    new_columns = {}
    for col in df.columns:
        if col.endswith("-1"):
            week_num = col.split("-")[0]
            new_columns[col] = f"第{week_num}周零售均价"
        elif col.endswith("-2"):
            week_num = col.split("-")[0]
            new_columns[col] = f"第{week_num}周系数"
        elif col.isdigit():
            new_columns[col] = f"第{col}周销量"
        elif col == "product_name":
            new_columns[col] = "品名"

    return df.rename(columns=new_columns)

def reorder_columns(df):
    """
    Function to reorder columns according to the specified sequence:
    - Start with '品名'
    - Followed by '第X周销量', '第X周零售均价', and '第X周系数' for each week from 1 to 52
    """
    # Starting with '品名'
    ordered_columns = ['品名']

    # Adding columns for each week
    for week in range(1, 53):
        ordered_columns.extend([f"第{week}周销量", f"第{week}周零售均价", f"第{week}周系数"])

    # Filter out columns that are not in the dataframe
    ordered_columns = [col for col in ordered_columns if col in df.columns]

    return df[ordered_columns]

# Load the Excel files
file_path_1 = '/Users/huzhang/Documents/我的 Tableau Prep 存储库/数据源/销量系数表_古月.xlsx'
file_path_2 = '/Users/huzhang/Documents/我的 Tableau Prep 存储库/数据源/销量系数表_晓望.xlsx'

# Read all sheets from both files
sheets_1 = pd.read_excel(file_path_1, sheet_name=None)
sheets_2 = pd.read_excel(file_path_2, sheet_name=None)

# Apply the renaming and reordering functions to all sheets in both Excel files
sheets_1_processed = {sheet_name: rename_columns(df) for sheet_name, df in sheets_1.items()}
sheets_2_processed = {sheet_name: rename_columns(df) for sheet_name, df in sheets_2.items()}

sheets_1_reordered = {sheet_name: reorder_columns(df) for sheet_name, df in sheets_1_processed.items()}
sheets_2_reordered = {sheet_name: reorder_columns(df) for sheet_name, df in sheets_2_processed.items()}


with pd.ExcelWriter(file_path_1) as writer:
    for sheet_name, df in sheets_1_reordered.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

with pd.ExcelWriter(file_path_2) as writer:
    for sheet_name, df in sheets_2_reordered.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

