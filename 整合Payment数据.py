import xlwings as xw
import os


def bianli(rootDir):
    os_list = []
    for root, dirs, files in os.walk(rootDir):
        for file in files:
            os_path = os.path.join(root, file)
            os_list.append(os_path)
        for dir in dirs:
            bianli(dir)
    return os_list


def collect_pay(sht, write_row,sht1,wb1):


    lastrow = sht.used_range.last_cell.row
    lastcol = sht.used_range.last_cell.column
    blankrow = 0
    lastrow1 = 1

    while blankrow < 2:
        lastrow1 += 1
        if sht.range("A" + str(lastrow1)).value is None:
            blankrow += 1

    # 获取每条扣费项目起始行
    rem_num = sht.range("A1").value[21:]
    rem_date = sht.range("A3").value[5:]
    for row in range(6, lastrow1):
        write_row += 1
        PO = sht.range("B" + str(row)).value
        Amo = sht.range("D" + str(row)).value
        Invoice = sht.range("A" + str(row)).value
        paydate = sht.range("C" + str(row)).value
        storeID = sht.range("E" + str(row)).value
        odertype = sht.range("F" + str(row)).value

        sg_list = [
            rem_num,
            rem_date,
            Invoice,
            str(paydate),
            PO,
            Amo,
            storeID,
            odertype]

        sht1.range("A" + str(write_row)).value = sg_list
    wb1.save()

    return write_row


def collect_US(sht, write_row,sht2,wb2):
    lastrow = sht.used_range.last_cell.row
    lastcol = sht.used_range.last_cell.column
    # 获取每条扣费项目起始行
    rownum_list = []
    for i in range(1, int(lastrow) + 1):
        if sht.range("A" + str(i)).value == "Credit" or sht.range("A" + str(i)).value == "Deduction":
            rownum_list.append(i)
    rem_num = sht.range("A1").value[21:]
    rem_date = sht.range("A3").value[5:]

    for row in rownum_list:
        write_row += 1
        PO = sht.range("B" + str(row)).value
        Amo = sht.range("D" + str(row)).value
        Item = sht.range("A" + str(row + 1)).value[5:]
        Qty = sht.range("B" + str(row + 1)).value[4:]
        csm = sht.range("A" + str(row + 2)).value[9:]
        try:
            rea = sht.range("B" + str(row + 2)).value[7:]
        except BaseException:
            rea = 'null'
        try:
            desc = sht.range("A" + str(row + 4)).value[5:]
            RA = sht.range("A" + str(row + 3)).value[4:]
        except BaseException:
            desc = sht.range("A" + str(row + 3)).value[5:]
            RA = 'null'
        remdate = sht.range("C" + str(row)).value
        sg_list = [
            rem_num,
            rem_date,
            RA,
            remdate,
            PO,
            Amo,
            Item,
            Qty,
            csm,
            rea,
            desc]

        sht2.range("A" + str(write_row)).value = sg_list
    wb2.save()

    return write_row


def collect_ca(sht, write_row,sht2,wb2):
    lastrow = sht.used_range.last_cell.row
    lastcol = sht.used_range.last_cell.column
    # 获取每条扣费项目起始行
    rownum_list = []
    for i in range(1, int(lastrow) + 1):
        if sht.range("A" + str(i)).value == "Credit" or sht.range("A" + str(i)).value == "Deduction":
            rownum_list.append(i)
    rem_num = sht.range("A1").value[21:]
    rem_date = sht.range("A3").value[5:]

    for row in rownum_list:
        write_row += 1

        PO = sht.range("B" + str(row)).value
        Amo = sht.range("E" + str(row)).value
        year = sht.range("D" + str(row)).value
        if Amo == '':
            Amo = sht.range("D" + str(row)).value
            year = "2021"
        Item = sht.range("A" + str(row + 1)).value[5:]
        Qty = sht.range("B" + str(row + 1)).value[4:]
        csm = sht.range("A" + str(row + 2)).value[9:]
        try:
            rea = sht.range("B" + str(row + 2)).value[7:]
        except:
            rea='null'
        try:
            desc = sht.range("A" + str(row + 4)).value[5:]
            RA = sht.range("A" + str(row + 3)).value[4:]
        except BaseException:
            desc = sht.range("A" + str(row + 3)).value[5:]
            RA = 'null'
        remdate = str(year) + str(sht.range("C" + str(row)).value)[4:]

        sg_list = [
            rem_num,
            rem_date,
            RA,
            remdate,
            PO,
            Amo,
            Item,
            Qty,
            csm,
            rea,
            desc,
            year]
        sht2.range("A" + str(write_row)).value = sg_list
    wb2.save()
    return write_row

    # df = pd.read_csv("Wayfair_Remittance_3737923.csv",header=0,delimiter="\t")
    # print(df.loc['Credit'])
    # print("获取到所有的值:\n{0}".format(df))


if __name__ == '__main__':

    app = xw.App(visible=True, add_book=False)
    newbook1 = r"C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\水单核对\CA_PAY.xlsx"
    newbook2 = r"C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\水单核对\CA_DUC.xlsx"
    # newbook1 = r"C:\Users\Admin\Downloads\CA_PAY.xlsx"
    # newbook2 = r"C:\Users\Admin\Downloads\CA_DUC.xlsx"
    newbook_list = [newbook1, newbook2]
    # dir_list = [
    #     r'E:\OneDrive\广新\售后报告\2022 Wayfair payment\US 22',
    #     r'E:\OneDrive\广新\售后报告\2022 Wayfair payment\US 22']
    # dir_list = [
    #     r'E:\OneDrive\广新\售后报告\2022 Wayfair payment\US 22-' + month,
    #     r'E:\OneDrive\广新\售后报告\2022 Wayfair payment\US 22-' + month,
    #     r'E:\OneDrive\广新\售后报告\2022 Wayfair payment\CA 22-' + month,
    #     r'E:\OneDrive\广新\售后报告\2022 Wayfair payment\CA 22-' + month]


    wb1 = app.books.add()
    sht1 = wb1.sheets[0]
    wb2 = app.books.add()
    sht2 = wb2.sheets[0]
    write_row1 = 1
    write_row2 = 1
    header_list1 = [
        'rem_num',
        'rem_date',
        'Invoice',
        'paydate',
        'PO',
        'Amo',
        'storeID',
        'odertype']
    sht1.range('A1').value = header_list1
    header_list2 = [
        'rem_num',
        'rem_date',
        'RA',
        'remdate',
        'PO',
        'Amo',
        'Item',
        'Qty',
        'csm',
        'rea',
        'desc']
    sht2.range('A1').value = header_list2
    csv_list = bianli(r'C:\Users\Admin\Nutstore\1\「晓望集群」\S数据分析\水单核对\CA')

    for csvdoc in csv_list:
        print('进度：{}/{}'.format(str(csv_list.index(csvdoc)),str(len(csv_list))))
        wb = app.books.open(csvdoc)
        sht = wb.sheets[0]
        write_row1 = collect_pay(sht, write_row1,sht1,wb1)
        write_row2 = collect_ca(sht, write_row2,sht2,wb2)
        # write_row2 = collect_US(sht, write_row2,sht2,wb2)
        wb.close()

    wb1.save(newbook_list[0])
    wb1.close()
    wb2.save(newbook_list[1])
    wb2.close()
    app.quit()
