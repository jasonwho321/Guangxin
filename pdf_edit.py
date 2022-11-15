import os
import fitz
from PIL import ImageFile,Image
import pdfplumber
import cv2

def puttext():
    bk_img = cv2.imread(r"C:\Users\Admin\Desktop\package_label\box of 1100 new\box of 1100 new_00.png")
    cv2.putText(bk_img,'1749',(420,920),cv2.FONT_HERSHEY_SIMPLEX,4,(0,0,0),15,cv2.LINE_AA)
    cv2.putText(bk_img,"1750",(940,920),cv2.FONT_HERSHEY_SIMPLEX,4,(0,0,0),15,cv2.LINE_AA)
    cv2.imwrite(r"C:\Users\Admin\Desktop\package_label\{}.jpg".format("1750"),bk_img)

def pic2pdf(img_path, pdf_path):
    file_list = os.listdir(img_path)
    for x in file_list:
        if "jpg" in x or 'png' in x or 'jpeg' in x:
            pdf_name = x.split('.')[0]
            im1 = Image.open(os.path.join(img_path, x))
            im1.save(pdf_path + pdf_name + '.pdf', "PDF", resolution=100.0)


def pdf_image(pdfPath,imgPath,zoom_x,zoom_y,rotation_angle):
    # 打开PDF文件
    pdf = fitz.open(pdfPath)
    # 逐页读取PDF
    for pg in range(0, pdf.pageCount):
        page = pdf[pg]
        # 设置缩放和旋转系数
        trans = fitz.Matrix(zoom_x, zoom_y).preRotate(rotation_angle)
        pm = page.getPixmap(matrix=trans, alpha="False")
        # 开始写图像
        pm.writePNG(imgPath+str(pg)+".png")
        #pm.writePNG(imgPath)
    pdf.close()

with pdfplumber.open(r"C:\Users\Admin\Documents\Tencent Files\544409644\FileRecv\Poundex\表格类\210901 2022 Motion Collection .pdf") as pdf:
    first_page = pdf.pages[0]
    print(first_page.chars[0])

for i in range(1749):
    print("进度：{}/{}".format(str(i+1),"1750"))
    bk_img = cv2.imread(r"C:\Users\Admin\Desktop\package_label\box of 1100 new\box of 1100 new_00.png")
    cv2.putText(bk_img,str(i+1),(420,920),cv2.FONT_HERSHEY_SIMPLEX,4,(0,0,0),15,cv2.LINE_AA)
    cv2.putText(bk_img,"1750",(940,920),cv2.FONT_HERSHEY_SIMPLEX,4,(0,0,0),15,cv2.LINE_AA)
    cv2.imwrite(r"C:\Users\Admin\Desktop\package_label\{}.jpg".format(str(i+1)),bk_img)



def rea(path, pdf_name):
    """
    :param path: 图片文件夹路径
    :param pdf_name: 输出PDF
    :return: 打印文件名称
    """
    # 自动查询文件夹内的图片文件
    # file_list = os.listdir(path)
    # pic_name = []
    # im_list = []
    # for x in file_list:
    #     if "jpg" in x or 'png' in x or 'jpeg' in x:
    #         pic_name.append(x)
    #
    # new_pic = []
    #
    # for x in pic_name:
    #     if "jpg" in x:
    #         new_pic.append(x)
    #
    # for x in pic_name:
    #     if "png" in x:
    #         new_pic.append(x)
    #
    # print("总计", new_pic)

    # 已有确定的命名规则
    new_pic = []
    im_list = []
    for i in range(1000,1749):
        new_pic.append("{}.jpg".format(str(i+2)))

    im1 = Image.open(os.path.join(path, new_pic[0]))
    new_pic.pop(0)

    for i in new_pic:
        img = Image.open(os.path.join(path, i))
        # im_list.append(Image.open(i))
        if img.mode == "RGBA":
            img = img.convert('RGB')
            im_list.append(img)
        else:
            im_list.append(img)

    im1.save(pdf_name, "PDF", resolution=100.0, save_all=True, append_images=im_list)
    print("输出文件名称：", pdf_name)


if __name__ == '__main__':
    pdf_name = r'C:\Users\Admin\Desktop\package_label\package_label_10001-1750.pdf'
    mypath=r"C:\Users\Admin\Desktop\package_label"
    if ".pdf" in pdf_name:
        rea(mypath, pdf_name=pdf_name)
    else:
        rea(mypath, pdf_name="{}_1-1000.pdf".format(pdf_name))