# -*- coding: UTF-8 -*-

import os
import time

from pdf2txt import get_txt_from_pdf, save_txt
from pdf2txt_format import txt_format_main


def get_file_list(path):  # 获取指定路径下的文件名列表
    file_name_list = []
    for file_name in os.listdir(path):
        if file_name[:-4] not in file_name_list:
            file_name_list.append(file_name[:-4])
        else:
            continue
    return file_name_list


def pdf_to_txt_main(file_name, pdf_path, temp_txt_path, formated_txt_path):

    pdf_full_path = pdf_path + '\\' + file_name + '.pdf'
    temp_txt_full_path = temp_txt_path + '\\' + file_name + '.txt'
    formated_txt_full_path = formated_txt_path + '\\' + file_name + '.txt'

    txt_list = get_txt_from_pdf(pdf_full_path)
    save_txt(txt_list, temp_txt_full_path)
    txt_format_main(temp_txt_full_path, formated_txt_full_path)  # 最终得到格式化后的txt文件

    return 0


if __name__ == "__main__":

    pdf_path = r'C:\Users\HRL\Desktop\APT报告威胁情报项目\pdf转txt\pdf测试数据'
    temp_txt_path = r'C:\Users\HRL\Desktop\APT报告威胁情报项目\pdf转txt\txt临时文件'
    formated_txt_path = r'C:\Users\HRL\Desktop\APT报告威胁情报项目\pdf转txt\txt格式化文件'

    st = time.time()
    file_name_list = get_file_list(pdf_path)
    for file_name in file_name_list:
        pdf_to_txt_main(file_name, pdf_path, temp_txt_path, formated_txt_path)
    et = time.time()
    print('{} files use {} seconds'.format(len(file_name_list), et-st))  # 13个pdf文件转为txt文件用时26.3秒 (有缓存24.2秒)
