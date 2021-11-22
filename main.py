# 这是一个示例 Python 脚本。

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import xlwings as xw

def print_hi(name):
    # 在下面的代码行中使用断点来调试脚本。
    print(f'Hi, {name}')  # 按 Ctrl+F8 切换断点。


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    app = xw.App(visible=True,add_book=False)
    book = app.books.open(r'E:\OneDrive\广新\SKU打标\BulkEdit_13926_汇总.xlsx')
    book1 = app.books.open(r'E:\OneDrive\广新\SKU打标\标签出现次数.xlsx')
    i=0
    con_list = []
    content_lon_list = []
    newcon = []
    while True:
        try:
            sht = book.sheets[i]
            # print(sht.used_range.shape[1])
            range1 = sht.used_range.shape[1]
            range2 = sht.used_range.shape[0]
            print(sht.name,range2)
            content = sht.range((3,7),(3,range1)).value
            content1 = sht.range((2, 7), (2, range1)).value
            content2 = sht.range((4, 7), (4, range1)).value
            content3 = sht.range((5, 7), (5, range1)).value
            content4 = sht.range((6, 7), (6, range1)).value
            for g in range(len(content)):
                if content[g] not in newcon:
                    newcon.append(content[g])
                    content_lon_list.append([content[g],content1[g],content2[g],content3[g],content4[g]])

            con_list.extend(content)
            i += 1
        except Exception as e:
            print(i)
            print(e)
            break
    # sht1 = book1.sheets[2]
    # sht1.range("A1").value = content_lon_list

    # new_list = []
    # new_dict = {}
    # for c in con_list:
    #     if c in new_list:
    #         new_dict[c] += 1
    #     else:
    #         new_list.append(c)
    #         new_dict[c]=1
    # # print(new_dict)
    # n = 1
    # for i in new_dict:
    #     book1.sheets[0].range("A"+str(n)).value = [i,new_dict[i]]
    #     n += 1


# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
