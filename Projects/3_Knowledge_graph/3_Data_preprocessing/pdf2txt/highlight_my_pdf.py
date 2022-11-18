# -*- coding: UTF-8 -*-

'''pip install PyMuPDF==1.16.7'''
import os
import fitz
import json
import time


def read_ioc(ioc_path):
    with open(ioc_path, 'r', encoding='utf-8') as f:
        return f.read()


def highlight_my_pdf(pdf_path, ioc_list, color):
    doc = fitz.Document(pdf_path)
    for page in doc:
        for ioc in ioc_list:
            text_instances = page.searchFor(ioc)  # 在每一页搜索目标文本。如果没有，则返回空列表。
            if text_instances:
                for inst in text_instances:
                    highlight = page.addHighlightAnnot(inst)  # 高亮显示
                    highlight.setColors({"stroke": color})
                    highlight.update()
            else:
                continue
    doc.save(pdf_path, incremental=True, deflate=True, clean=True, encryption=0)
    return 0


def highlight_pdf_main(file_name, pdf_path, ioc_result_path, color):
    pdf_full_path = pdf_path + '\\' + file_name + '.pdf'
    ioc_result_full_path = ioc_result_path + '\\' + file_name + '.txt'

    ioc_result = read_ioc(ioc_result_full_path)
    ioc_dict = json.loads(ioc_result)
    content = json.loads(ioc_dict['ioc_result'])
    for key in list(content.keys()):
        ioc_list = content.get(key)
        ioc_color = color.get(key)
        highlight_my_pdf(pdf_full_path, ioc_list, ioc_color)
    return 0


def get_file_list(path):  # 获取指定路径下的文件名列表
    file_name_list = []
    for file_name in os.listdir(path):
        if file_name[:-4] not in file_name_list:  # 去掉后4位
            file_name_list.append(file_name[:-4])
        else:
            continue
    return file_name_list


if __name__ == "__main__":

    file_name = r'【奇安信】盲眼鹰（APT-C-36）针对哥伦比亚政企机构的攻击活动揭露'
    pdf_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\PDF高亮\pdf高亮数据'
    ioc_result_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\IOC正则抽取\IOC结果文件'

    # color_list = ['purple', 'blue', 'green', 'red', 'pink', 'yellow', 'brown', 'grey', 'cyan']  # 颜色与IOC的对应关系
    color = {'ip':(0.52, 0.44, 1), 'domain':(0, 0.75, 1), 'md5':(0.6, 0.98, 0.6), 'sha1':(0.7, 0.13, 0.13),
             'sha256':(1, 0.75, 0.80), 'url':(1, 1, 0), 'filename':(1, 0.87, 0.68), 'Windows_path':(0.75, 0.75, 0.75),
             'registry_path':(0, 1, 1)}

    st = time.time()
    file_name_list = get_file_list(ioc_result_path)
    for file_name in file_name_list:
        highlight_pdf_main(file_name, pdf_path, ioc_result_path, color)
    et = time.time()
    print('{} files use {} seconds'.format(len(file_name_list), et-st))  # 13个文件用时56.3秒
