import xlwings as xw

if __name__ == '__main__':
    app = xw.App(visible=True,add_book=False)
    book = app.books.open(r'E:\OneDrive\广新\SKU打标\211011 晓望在仓SKU.xlsx')
    book1 = app.books.open(r'E:\OneDrive\广新\SKU打标\BulkEdit_13926_汇总.xlsx')
    sht = book.sheets[0]
    sku_list = sht.range('B2:B1034').value
    cat_list = sht.range('C1:AH1').value
    for sku in sku_list:
        i = 0
        while True:
            try:
                sht1 = book1.sheets[i]
                range1 = sht1.used_range.shape[0]
                range2 = sht1.used_range.shape[1]
                lookup_list = sht1.range("B7:B"+str(range1)).value
                look_cat_list = sht1.range((3, 7), (3, range2)).value
                if sku in lookup_list:
                    row_loc = lookup_list.index(sku)+7
                    for cat in cat_list:
                        if cat in look_cat_list:
                            col_loc = look_cat_list.index(cat) + 7
                            fill_row_loc = sku_list.index(sku)+2
                            fill_col_loc = cat_list.index(cat) +3
                            sht.range((fill_row_loc, fill_col_loc)).value = sht1.range((row_loc, col_loc)).value

                i += 1
            except Exception as e:
                print(i)
                print(e)
                break