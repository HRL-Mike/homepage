# -*- coding: UTF-8 -*-

import re


def read_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.readlines()


def format_txt(lines):
    new_lines = []
    for line in lines:
        new_lines.append(line.strip('\n'))
    txt = ''.join(new_lines)

    # 切分句子
    sentence_list = []
    new_txt = txt
    while True:
        sentence = re.search(r'\.[ ]+[A-Z]', new_txt)
        if sentence is None:
            sentence_list.append(new_txt)
            break
        else:  # 包含至少一个句子
            pos = sentence.span()  # (149, 152)
            if new_txt[pos[0]+1] == ' ':
                sentence = new_txt[:pos[0]+1]
                sentence_list.append(sentence)
                new_txt = new_txt[pos[1]-1:]
            else:
                print('无法匹配空格')
    print(sentence_list)

    # for sentence in sentence_list:
    #     if sentence[0] == ' ':
    #         print('wrong')

    return sentence_list


def write_to_file(txt, path):
    with open(path, 'w', encoding='utf-8') as f:
        for line in txt:
            f.write(line + '\n')
    return 0


def txt_format_main(temp_txt_full_path, formated_txt_full_path):

    txt_list = read_data(temp_txt_full_path)
    sentence_list = format_txt(txt_list)
    write_to_file(sentence_list, formated_txt_full_path)


if __name__ == "__main__":

    file_name = r'0001'
    temp_txt_full_path = r'C:\Users\HRL\Desktop\APT报告威胁情报项目\pdf转txt\txt临时文件' + '\\' + file_name + '.txt'
    formated_txt_full_path = r'C:\Users\HRL\Desktop\APT报告威胁情报项目\pdf转txt\txt格式化文件' + '\\' + file_name + '.txt'

    txt_format_main(temp_txt_full_path, formated_txt_full_path)
