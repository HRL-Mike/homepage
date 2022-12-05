# -*- coding: UTF-8 -*-

from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.layout import LAParams, LTTextBox, LTFigure, LTImage
from pdfminer.converter import PDFPageAggregator


def get_txt_from_pdf(pdf_full_path):
    with open(pdf_full_path, 'rb') as f:
        parser = PDFParser(f)
        doc = PDFDocument(parser)
        rsrc_mgr = PDFResourceManager()
        layout_para = LAParams(char_margin=20, line_margin=15)  # 这两个参数的设置取决于PDF内容的布局，可缓解乱序问题
        device = PDFPageAggregator(rsrc_mgr, laparams=layout_para)
        interpreter = PDFPageInterpreter(rsrc_mgr, device)

        txt_list = []
        for page in PDFPage.create_pages(doc):
            interpreter.process_page(page)
            layout = device.get_result()
            for x in layout:
                # 获取文本对象
                if isinstance(x, LTTextBox):
                    txt_list.append(x.get_text())  # 存在局部错位（靠LAParams的参数解决）
    return txt_list


def save_txt(txt_list, txt_path):
    with open(txt_path, 'w', encoding='utf-8') as tf:
        for line in txt_list:
            tf.writelines(line)


if __name__ == "__main__":

    file_name = r'【奇安信】血茜草永不停歇的华语情报搜集活动'
    pdf_full_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\pdf转txt\pdf测试数据' + '\\' + file_name + '.pdf'
    temp_txt_full_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\pdf转txt\txt临时文件' + '\\' + file_name + '.txt'
    # image_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\pdf转txt\东巽科技丰收行动'

    txt_list = get_txt_from_pdf(pdf_full_path)
    save_txt(txt_list, temp_txt_full_path)

    '''看看有没有更好的库，提取pdf的图片'''
