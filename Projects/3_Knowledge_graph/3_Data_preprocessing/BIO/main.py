# -*- coding=utf-8 -*-

from generate_BIO import generate_bio
from integrate_label import integrate_label

if __name__ == '__main__':

    path = r'C:\Users\herunlong\Desktop\BIO相关\data'  # 存放.txt, .ann和.bio文件的文件夹
    generate_bio(path)
    integrate_label(path)
