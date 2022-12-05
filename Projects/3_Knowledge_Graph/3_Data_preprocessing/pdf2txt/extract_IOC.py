# -*- coding: UTF-8 -*-

import os
import re
import json
import time

from ip_and_domain_filter import filter_ip, filter_domain


ip_pattern = re.compile(r'\b\d{2,3}(?:\.|\[\.\]){1}\d{1,3}(?:\.|\[\.\]){1}\d{1,3}(?:\.|\[\.\]){1}\d{1,3}\b')
md5_pattern = re.compile(r'\b[a-fA-F0-9]{32}\b')
sha1_pattern = re.compile(r'\b[a-fA-F0-9]{40}\b')
sha256_pattern = re.compile(r'\b[a-fA-F0-9]{64}\b')
url_pattern = re.compile(r'\b[a-zA-Z]+://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]\b')
# 可添加新的文件扩展名
filename_pattern = re.compile(r'(?i)\b[a-zA-Z0-9-_.\u4e00-\u9fa5]+\.(?:txt|php|exe|dll|bat|sys|htm|html|js|jar|jpg|png|vb|scr|pif'
                              r'|chm|zip|rar|cab|pdf|doc|docx|ppt|pptx|xls|xlsx|swf|gif|bin|bmp|jpeg|svg|raw|wmf|tif'
                              r'|psd|tga|fpx|ufo|eps|hdri|flic|emf|ico|exif|gz|mp3|mp4|au|wav|wma|aac|flac|avi|adt'
                              r'|cmd|c|asm|lib|for|lst|msg|obj|pas|wki|bas|java|go|py|cpp|h|dsp|out|map|tmp|reg|iso'
                              r'|sql|rmvb|rm|mpg|asf|css|ppsx)\b')  # 不能识别带空格的文件名,文件名仅支持中文和英文
# 包括通用顶级域和国家顶级域，可添加新的顶级域
domain_pattern = re.compile(r'\b[-a-zA-Z0-9]{1,62}(?:(?:\.|\[\.\]){1}[-a-zA-Z0-9]{1,62})*(?:\.|\[\.\]){1}(?:com|edu|gov'
                            r'|int|mil|net|org|biz|info|pro|name|museum|coop|aero|xxx|idv|cn|eu|uk|us|fr|de|al|dz|af'
                            r'|ar|ae|aw|om|az|eg|et|ie|ee|ad|ao|ai|ag|at|au|mo|bb|pg|bs|pk|py|ps|bh|pa|br|by|bm|bg|mp'
                            r'|bj|be|is|pr|ba|pl|bo|bz|bw|bt|bf|bi|bv|kp|gq|dk|tl|tp|tg|dm|do|ru|ec|er|fo|pf|gf|tf|va'
                            r'|ph|fj|fi|cv|fk|gm|cg|cd|co|cr|gg|gd|gl|ge|cu|gp|gu|gy|kz|ht|kr|nl|an|hm|hn|ki|dj|kg|gn'
                            r'|gw|ca|gh|ga|kh|cz|zw|cm|qa|ky|km|ci|kw|cc|hr|ke|ck|lv|ls|la|lb|lt|lr|ly|li|re|lu|rw|ro'
                            r'|mg|im|mv|mt|mw|my|ml|mk|mh|mq|yt|mu|mr|um|as|vi|mn|ms|bd|pe|fm|mm|md|ma|mc|mz|mx|nr|np'
                            r'|ni|ne|ng|nu|no|nf|na|za|aq|gs|pw|pn|pt|jp|se|ch|sv|ws|yu|sl|sn|cy|sc|sa|cx|st|sh|kn|lc'
                            r'|sm|pm|vc|lk|sk|si|sj|sz|sd|sr|sb|so|tj|tw|th|tz|to|tc|tt|tn|tv|tr|tm|tk|wf|vu|gt|ve|bn'
                            r'|ug|ua|uy|uz|es|eh|gr|hk|sg|nc|nz|hu|sy|jm|am|ac|ye|iq|ir|il|it|in|id|vg|io|jo|vn|zm|je'
                            r'|td|gi|cl|cf)\b')
# 支持中文和英文路径，最后一个“\”后不允许带空格
win_path_pattern = re.compile(r'\b[a-zA-Z]:\\(?:[-.\w \u4e00-\u9fa5]*[\\])*[-.\w\u4e00-\u9fa5]*\b')

# 未测试
registry_path_pattern = re.compile(r'\b((?:HKEY_CLASSES_ROOT|HKEY_CURRENT_USER|HKEY_LOCAL_MACHINE|HKEY_USERS|HKEY_CURRENT_CONFIG)(?:\\.+)+)\b')
# linux_path_pattern = re.compile(r'\b(?:/[-\w .]+)+\b')  不可用


def extract_ioc_and_location(text, pattern):
    ioc_list = []
    ioc_location = []
    for item in pattern.finditer(text):
        ioc_list.append(item.group())
        ioc_location.append(item.span())
    return ioc_list, ioc_location


def extract_ioc(text, pattern):
    ioc_list = pattern.findall(text)
    return ioc_list


def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        txt = f.readlines()
        # print(txt)
    return txt


def store_into_list(ioc_list, total_list):
    for item in ioc_list:
        if item not in total_list:
            total_list.append(item)
    return total_list


def write_to_file(file_path, result_dict):
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(result_dict))
    return 0


def extract_ioc_main(file_name, formated_txt_path, ioc_result_path):
    # 构造完整路径
    formated_txt_full_path = formated_txt_path + '\\' + file_name + '.txt'
    ioc_result_full_path = ioc_result_path + '\\' + file_name + '.txt'

    all_ip_list = []
    all_domain_list = []
    all_md5_list = []
    all_sha1_list = []
    all_sha256_list = []
    all_url_list = []
    all_filename_list = []
    all_win_path_list = []
    all_registry_path_list = []

    file_txt = read_file(formated_txt_full_path)
    for line in file_txt:
        line = line.strip()
        ip_list, ip_location = extract_ioc_and_location(line, ip_pattern)  # 提取ip
        domain_list, domain_location = extract_ioc_and_location(line, domain_pattern)  # 提取域名
        md5_list = extract_ioc(line, md5_pattern)  # 提取md5
        sha1_list = extract_ioc(line, sha1_pattern)  # 提取sha1
        sha256_list = extract_ioc(line, sha256_pattern)  # 提取sha256
        url_list = extract_ioc(line, url_pattern)  # 提取url
        windows_path_list = extract_ioc(line, win_path_pattern)  # 提取Windows路径
        registry_path_list = extract_ioc(line, registry_path_pattern)  # 提取注册表路径
        filename_list = extract_ioc(line, filename_pattern)  # 提取文件名

        # 将每一行的结果整合进一个列表
        if ip_list:
            all_ip_list = store_into_list(ip_list, all_ip_list)
        if domain_list:
            all_domain_list = store_into_list(domain_list, all_domain_list)
        if md5_list:
            all_md5_list = store_into_list(md5_list, all_md5_list)
        if sha1_list:
            all_sha1_list = store_into_list(sha1_list, all_sha1_list)
        if sha256_list:
            all_sha256_list = store_into_list(sha256_list, all_sha256_list)
        if url_list:
            all_url_list = store_into_list(url_list, all_url_list)
        if filename_list:
            all_filename_list = store_into_list(filename_list, all_filename_list)
        if windows_path_list:
            all_win_path_list = store_into_list(windows_path_list, all_win_path_list)
        if registry_path_list:
            all_registry_path_list = store_into_list(registry_path_list, all_registry_path_list)

    # 对IP和Domain进行过滤
    with open(formated_txt_full_path, 'r', encoding='utf-8') as f:
        txt = f.read()
    txt = txt.replace('\n', '')
    txt = txt.replace(' ', '')
    txt = txt.replace(' ', '')
    filtered_ip_list = filter_ip(all_ip_list)
    filtered_domain_list = filter_domain(txt, all_domain_list)

    # 写入文件
    key_list = ['ip', 'domain', 'md5', 'sha1', 'sha256', 'url', 'filename', 'Windows_path', 'registry_path']
    ioc_result_list = [filtered_ip_list, filtered_domain_list, all_md5_list, all_sha1_list, all_sha256_list,
                       all_url_list, all_filename_list, all_win_path_list, all_registry_path_list]
    temp_dict = {}
    for i, key in enumerate(key_list):
        temp_dict[key] = ioc_result_list[i]
    result_dict = {'ioc_result': json.dumps(temp_dict)}
    write_to_file(ioc_result_full_path, result_dict)
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

    formated_txt_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\pdf转txt\txt格式化文件'
    ioc_result_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\IOC正则抽取\IOC结果文件'

    st = time.time()
    file_name_list = get_file_list(formated_txt_path)
    for file_name in file_name_list:
        extract_ioc_main(file_name, formated_txt_path, ioc_result_path)
    et = time.time()
    print('{} files use {} seconds'.format(len(file_name_list), et-st))  # 13个文件用时0.23秒

    # import py_compile
    # py_compile.compile(r'H:/game/test.py')
    # import compileall
    # compileall.compile_dir(r'H:/game')

    # a = '我的 192.168.1.1 外人'
    # i = re.compile(r'\b\d{2,3}(?:\.|\[\.\]){1}\d{1,3}(?:\.|\[\.\]){1}\d{1,3}(?:\.|\[\.\]){1}\d{1,3}\b')
    # ip_list, ip_location = extract_ioc_and_location(a, i)
    # print(ip_list)
    # '''还是得加\b, 或者在中文和数字&英文之间加空格'''
