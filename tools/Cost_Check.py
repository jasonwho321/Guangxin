import os
from base import get_engine
import pandas as pd
import datetime

def check_cost():
    df = pd.read_csv('output.csv',encoding='gb18030')
    # Add date_ver column with today's date
    today = datetime.date.today().strftime('%Y-%m-%d')
    df['date_ver'] = today

    # Write merged_df to the database
    engine = get_engine()
    df.to_sql('COGS_Calculation', schema='Product_Mgmt', con=engine, if_exists='append', index=False)

    print("Data has been successfully inserted into 39F.Product_Mgmt.COGS_Calculation table.")
    os.remove('output.csv')
    engine.dispose()
def mapping_download(filedir = '/Users/huzhang/Desktop/FOB DDP.xlsx'):
    df_input = pd.read_excel(filedir)
    first_column = df_input.iloc[:, 0].tolist()
    placeholders = ','.join(['?'] * len(first_column))
    sql_query = f"""
    WITH ranked_items AS (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY item_number ORDER BY P_ID DESC) as row_num
        FROM dbo.COGS_0113
        WHERE item_number IN ({placeholders})
    )
    SELECT *
    FROM ranked_items
    WHERE row_num = 1
    """
    engine = get_engine()
    query_result = pd.read_sql(sql_query, con=engine, params=first_column)
    # Create a DataFrame with all item_numbers from first_column
    all_item_numbers = pd.DataFrame(first_column, columns=['Item_Number'])
    # Left join the query result with the all_item_numbers DataFrame
    merged_df = all_item_numbers.merge(query_result, on='Item_Number', how='left')
    df = merged_df.drop(['row_num', 'P_ID'], axis=1)
    engine.dispose()
    df.to_csv('output.csv', encoding='gb18030', index=False)



if __name__ == '__main__':
    mapping_download('')
    check_cost()