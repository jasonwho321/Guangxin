import os
import xlwings as xw

app = xw.App(visible=True,add_book=False)
book = app.books.add()
sht = book.sheets[0]

g = os.walk(r"Z:\『晓望集群』\『晓望梅观』\Morning Tree\ALL")
i = 1
for path,dir_list,file_list in g:
    for file_name in file_list:
        full_path = os.path.join(path, file_name)
        sht.range("A"+str(i)).value = os.path.join(path, file_name)
        i+=1
        print(i)

book.save(r"Z:\『晓望集群』\『晓望梅观』\Morning Tree\ALL\文件夹汇总表0530.xlsx")
book.close()
app.quit()

