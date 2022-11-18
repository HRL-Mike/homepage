# -*- coding: UTF-8 -*-

import os

from search.es_search import insert_doc
from infoExtract.IOC.extract_IOC import extract_ioc_main
from infoExtract.PDF.pdf2txt import get_txt_from_pdf
from infoExtract.PDF.pdf2txt_format import txt_format_main
from infoExtract.PDF.highlight_my_pdf import highlight_pdf_main
from Utils.io_files import writelines


def pdf_to_txt_main(file_name, pdf_path, temp_txt_path, formated_txt_path):

    pdf_full_path = pdf_path + '/' + file_name + '.pdf'
    temp_txt_full_path = temp_txt_path + '/' + file_name + '.txt'
    formated_txt_full_path = formated_txt_path + '/' + file_name + '.txt'

    txt_list = get_txt_from_pdf(pdf_full_path)
    writelines(txt_list, temp_txt_full_path)
    txt_format_main(temp_txt_full_path, formated_txt_full_path)  # 最终得到格式化后的txt文件

    return 0


if __name__ == "__main__":
   
    # 存放高亮pdf的路径
    highlight_pdf_path = r'/opt/data/pdf_highlight_data'
    # 存放txt临时数据的路径
    temp_txt_path = r'/opt/data/txt_temp_data'
    # 存放格式化txt的路径
    formated_txt_path = r'/opt/data/txt_formated_data'
    # 存放ioc抽取结果的路径
    ioc_result_path = r'/opt/data/txt_ioc_result'
    # RGB颜色字典 ['purple', 'blue', 'green', 'red', 'pink', 'yellow', 'brown', 'grey', 'cyan']
    color = {'ip': (0.52, 0.44, 1), 'domain': (0, 0.75, 1), 'md5': (0.6, 0.98, 0.6), 'sha1': (0.7, 0.13, 0.13),
             'sha256': (1, 0.75, 0.80), 'url': (1, 1, 0), 'filename': (0.55, 0.27, 0), 'Windows_path': (0.75, 0.75, 0.75),
             'registry_path': (0, 1, 1)}

    file_name_list = []
    for file_name in file_name_list:
        pdf_to_txt_main(file_name, highlight_pdf_path, temp_txt_path, formated_txt_path)
        insert_doc(file_name)
        extract_ioc_main(file_name, formated_txt_path, ioc_result_path)
        highlight_pdf_main(file_name, highlight_pdf_path, ioc_result_path, color)
