# -*- coding: UTF-8 -*-

import re


def read_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.readlines()


def format_txt(lines):
    # 把带空格的换行符替换为换行符
    for i, line in enumerate(lines):
        if re.search(r'[\u0020\u00A0\u3000]+\n$', line):
            temp_str = re.sub(r'[\u0020\u00A0\u3000]+\n$', '\n', line)
            lines[i] = temp_str
        else:
            continue

    # 删除无用的换行符
    while '\n' in lines:
        lines.remove('\n')

    # 去除页码
    page_num = []
    for i in range(0, 50):
        page_num.append(str(i)+'\n')
    for i, line in enumerate(lines):
        if line in page_num:
            del lines[i]
    '''预处理结束'''

    # 如果换行符前面是中文，则去掉换行符
    for i, line in enumerate(lines):
        if i+1 < len(lines):
            if not re.search(r'[\u3001\u3002\uff01\uff0c\uff1a\uff1b\uff1f]+', line) and not re.search(r'[\u3001\u3002\uff01\uff0c\uff1a\uff1b\uff1f]+', lines[i+1]):
                continue  # 如果当前行和下一行都没有中文标点，则循环继续
            elif re.search(r'[\u3001\uff0c\uff1a\u4e00-\u9fa5]{1}\n$', line):  # ，、：和中文
                temp_str = re.sub(r'\n$', '', line)
                lines[i] = temp_str
            else:
                continue
        else:
            continue
    # print(lines)

    # 如果字符串包含中文符号，则在中文符号后加上换行符
    for i, line in enumerate(lines):
        if re.search(r'[。；？！]+', line):
            temp_str = re.sub(r'。', '。\n', lines[i])
            lines[i] = temp_str
            temp_str = re.sub(r'；', '；\n', lines[i])
            lines[i] = temp_str
            temp_str = re.sub(r'？', '？\n', lines[i])
            lines[i] = temp_str
            temp_str = re.sub(r'！', '！\n', lines[i])
            lines[i] = temp_str
        else:
            continue

    # 将双换行符替换为单换行符
    for i, line in enumerate(lines):
        if re.search(r'\n\n', line):
            temp_str = re.sub(r'\n\n', r'\n', lines[i])
            lines[i] = temp_str
        else:
            continue
    # print(lines)

    # 拼接字符串
    txt = ''
    txt = txt.join(lines)
    return txt


def write_to_file(txt, path):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(txt)
    return 0


def txt_format_main(temp_txt_full_path, formated_txt_full_path):

    txt_list = read_data(temp_txt_full_path)
    formated_txt = format_txt(txt_list)
    write_to_file(formated_txt, formated_txt_full_path)


if __name__ == "__main__":

    file_name = r'【奇安信】盲眼鹰（APT-C-36）针对哥伦比亚政企机构的攻击活动揭露'
    temp_txt_full_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\pdf转txt\txt临时文件' + '\\' + file_name + '.txt'
    formated_txt_full_path = r'C:\Users\herunlong\Desktop\APT报告威胁情报项目\pdf转txt\txt格式化文件' + '\\' + file_name + '.txt'

    txt_format_main(temp_txt_full_path, formated_txt_full_path)
