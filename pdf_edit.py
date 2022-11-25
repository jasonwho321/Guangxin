import os
import fitz
from PIL import ImageFile,Image
import cv2

def puttext(png_board_path,num_text,limit_text,path):
    bk_img = cv2.imread(png_board_path)
    cv2.putText(bk_img,num_text,(420,920),cv2.FONT_HERSHEY_SIMPLEX,4,(0,0,0),15,cv2.LINE_AA)
    cv2.putText(bk_img,limit_text,(940,920),cv2.FONT_HERSHEY_SIMPLEX,4,(0,0,0),15,cv2.LINE_AA)
    cv2.imwrite("{}\{}.jpg".format(path,"1750"),bk_img)

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




def rea(path, pdf_name,limit):
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
    k = 0
    while limit-500*k >= 500:
        for i in range(500*k+1,500*k+500):
            new_pic.append("{}.jpg".format(str(i)))

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
        im1.save('{}/{}{}.pdf'.format(out_put_path,pdf_name,str(500*(k+1))), "PDF", resolution=100.0, save_all=True, append_images=im_list)
        print("输出文件名称：", pdf_name)
        k+=1
    else:
        for i in range(500*k+1,limit):
            new_pic.append("{}.jpg".format(str(i)))

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
        im1.save('{}/{}{}.pdf'.format(out_put_path,pdf_name,str(500*(k+1))), "PDF", resolution=100.0, save_all=True, append_images=im_list)
        print("输出文件名称：", pdf_name)



if __name__ == '__main__':
    limit = 1500
    out_put_path = '/Users/huzhang/Desktop/output'
    pdf_name = 'package_label'

    for i in range(1,limit+1):
        print("进度：{}/{}".format(str(i + 1), str(limit)))
        puttext('',str(i),str(limit),out_put_path)
    rea(out_put_path, pdf_name,limit)
