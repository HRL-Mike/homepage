# -*- coding=utf-8 -*-

import os


def get_bio_file_list(data_path):

    file_list = []
    for filename in os.listdir(data_path):
        if filename[-4:] == '.bio':
            file_list.append(filename)
        else:
            continue
    return file_list


def integration_with_label(file):  # 根据标签整合ip, domain, CVE

    lines = file.readlines()

    # 获取bio文件内容列表
    content_list = []
    for line in lines:
        line = line.rstrip('\n').split(' ')
        content_list.append(line)
    # print(content_list)

    # 获取起止下标列表
    temp_list = []
    position_list = []
    start_label = ''
    for i in range(len(content_list)):
        if content_list[i][0]:
            if start_label and content_list[i][1] == 'O':
                start_label = ''
            if  content_list[i][1] in ['B-ip', 'B-domain', 'B-vulnerability', 'B-file', 'B-location']:
                start_label = content_list[i][1].split('-')[-1]
                temp_list.append(i)
            elif content_list[i][1] in ['I-ip', 'I-domain', 'I-vulnerability', 'I-file', 'I-location']:
                '''
                可以优化一下: 如果I-label包含特殊符号(./&/-)才整合，比如C&C
                '''
                if content_list[i+1][0]:
                    if content_list[i][1].split('-')[-1] == start_label and content_list[i+1][1] == 'O':
                        temp_list.append(i)
                        position_list.append(temp_list)
                        start_label = ''
                        temp_list = []
                    else:
                        continue
                else:
                    temp_list.append(i)
                    position_list.append(temp_list)
                    start_label = ''
                    temp_list = []
        else:
            continue
    # print(position_list)

    # 整合标签
    for index in position_list[::-1]:  # 倒序整合label
        ptr1 = index[0]
        ptr2 = index[1]
        token = ''
        label = content_list[ptr1][-1]
        while ptr1 <= ptr2:
            token = token + content_list[ptr1][0]
            ptr1 += 1
        insert_obj = [token, label]

        del content_list[index[0]:index[1]+1]  # 删除后，后面的下标会变，因此采用倒序整合
        content_list.insert(index[0], insert_obj)

    return  content_list


def write_to_file(f, content_list):

    for content in content_list:
        if content[0]:
            f.write(content[0] + ' ' + content[1] + '\n')
        else:
            f.write('\n')

    return 0


def integration_without_label(content_list):  # 整合月份缩写，不依赖标签 (标签为'O')

    month_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    position_list = []
    for i in range(len(content_list)):
        if content_list[i][0]:
           if content_list[i][0] in month_list:
               if content_list[i+1][0]:
                   if content_list[i+1][0] == '.':
                    position_list.append(i)
               else:
                   continue
           else:
               continue
        else:
            continue

    for subscript in position_list[::-1]:  # 倒序整合
        ptr1 = subscript
        ptr2 = subscript+1
        token = ''
        label = content_list[ptr1][-1]
        while ptr1 <= ptr2:
            token = token + content_list[ptr1][0]
            ptr1 += 1
        insert_obj = [token, label]

        del content_list[subscript:subscript+2]  # 切片操作的右区间为开区间
        content_list.insert(subscript, insert_obj)

    return content_list


def integrate_label(path):

    # 获取bio文件列表
    file_list = get_bio_file_list(path)

    # 读取bio文件，整合ip, domain, CVE标签，写入原文件
    for file in file_list:
        file_path = path + '\\' + file
        f = open(file_path, 'r', encoding='UTF-8')
        content_list = integration_with_label(f)
        content_list = integration_without_label(content_list)
        f.close()
        f = open(file_path, 'w', encoding='UTF-8')
        write_to_file(f, content_list)
        f.close()

    return 0


if __name__ == '__main__':

    path = r'C:\Users\herunlong\Desktop\BIO相关\data'  # 存放.bio文件的文件夹
    integrate_label(path)
