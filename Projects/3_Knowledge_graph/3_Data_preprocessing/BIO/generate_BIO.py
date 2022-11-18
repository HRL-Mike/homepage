# -*- coding=utf-8 -*-

import re
import os
import sys

from src.sentencesplit import sentencebreaks_to_newlines
from src.sentencesplit import _normspace
from src.ssplit import regex_sentence_boundary_gen


# compile函数将正则表达式编译成Pattern对象
TOKENIZATION_REGEX = re.compile(r'([0-9a-zA-Z]+|[^0-9a-zA-Z])')  # 分词正则表达式
NEWLINE_TERM_REGEX = re.compile(r'(.*?\n)')


def txt_to_bio(path, file_name):

    # 构造文件路径
    txt_path = path + '\\' + file_name + '.txt'
    ann_path = path + '\\' + file_name + '.ann'
    # print(txt_path)

    # 读取txt文件
    sentences = []
    with open(txt_path, 'r', encoding='UTF-8') as f:
        for l in f:
            l = sentencebreaks_to_newlines(l)  # 这个函数解决了"txt文件与ann文件无法对齐"的问题
            sentences.extend([s for s in NEWLINE_TERM_REGEX.split(l) if s])

    offset = 0
    new_lines = []
    for s in sentences:
        nonspace_token_seen = False
        tokens = [t for t in TOKENIZATION_REGEX.split(s) if t]
        # print(tokens)  # 单词列表

        for t in tokens:
            if not t.isspace():  # 如果不空
                new_lines.append([offset, offset + len(t), t, 'O'])
                nonspace_token_seen = True
            offset += len(t)  # 因为切片操作的右区间为开区间

        # sentences delimited by empty lines
        if nonspace_token_seen:
            new_lines.append([])
    # print(new_lines)  # [[0, 4, '0208', 'O'], [5, 11, 'United', 'O'], ...

    # add labels (other than 'O') from standoff annotation if specified
    new_lines = relabel(new_lines, get_annotations(ann_path))
    # print(new_lines)
    new_lines = [[str(l[0]), str(l[1]), l[2], l[3]] if l else l for l in new_lines]
    # print(new_lines)

    return new_lines


def relabel(lines, annotations):

    offset_label = {}
    for tb in annotations:
        # print(tb)  # [1140, 1153, 'Lazarus Group', 'attacker']
        for i in range(tb[0], tb[1]):
            if i in offset_label:
                print("Warning: overlapping annotations", file=sys.stderr)
            offset_label[i] = tb
    # print(offset_label)  # {1140: [1140, 1153, 'Lazarus Group', 'attacker'], ...}

    # print(lines)  # [[0, 4, '0208', 'O'], [5, 11, 'United', 'O'], ...]
    prev_label = None
    for i, l in enumerate(lines):
        if not l:
            prev_label = None
            continue
        start, end, token, tag = l

        # TODO: warn for multiple, detailed info for non-initial
        label = None
        for o in range(start, end):
            if o in offset_label:
                if o != start:
                    print('Warning: annotation-token boundary mismatch: "%s" --- "%s"' % (
                        token, offset_label[o].text), file=sys.stderr)
                label = offset_label[o][3]
                break

        if label is not None:
            if label == prev_label:
                tag = 'I-' + label
            else:
                tag = 'B-' + label
        prev_label = label

        lines[i] = [start, end, token, tag]
        # print(lines[i])

    return lines


def get_annotations(ann_path):

    with open(ann_path, 'r', encoding='UTF-8') as f:
        text_bounds = parse_text_bounds(f)  # 传入ann文件，返回
    # text_bounds = eliminate_overlaps(text_bounds)  # 去掉嵌套实体中更短的那个实体

    return text_bounds


TEXTBOUND_LINE_RE = re.compile(r'^T\d+\t')


def parse_text_bounds(f):

    text_bounds = []
    for l in f:
        l = l.rstrip('\n')
        if not TEXTBOUND_LINE_RE.search(l):
            continue
        id_, type_offsets, text = l.split('\t')
        type_, start, end = type_offsets.split()
        start, end = int(start), int(end)
        text_bounds.append([start, end, text, type_])
    # print(text_bounds)  # [[1140, 1153, 'Lazarus Group', 'attacker'], ... ]
    return text_bounds


def _text_by_offsets_gen(text, offsets):
    for start, end in offsets:
        yield text[start:end]


def sentencebreaks_to_newlines(text):

    offsets = [o for o in regex_sentence_boundary_gen(text)]

    # break into sentences
    sentences = [s for s in _text_by_offsets_gen(text, offsets)]

    # join up, adding a newline for space where possible
    orig_parts = []
    new_parts = []

    sentnum = len(sentences)
    for i in range(sentnum):
        sent = sentences[i]
        orig_parts.append(sent)
        new_parts.append(sent)

        if i < sentnum - 1:
            orig_parts.append(text[offsets[i][1]:offsets[i + 1][0]])

            if (offsets[i][1] < offsets[i + 1][0] and
                    text[offsets[i][1]].isspace()):
                # intervening space; can add newline
                new_parts.append(
                    '\n' + text[offsets[i][1] + 1:offsets[i + 1][0]])
            else:
                new_parts.append(text[offsets[i][1]:offsets[i + 1][0]])

    if len(offsets) and offsets[-1][1] < len(text):
        orig_parts.append(text[offsets[-1][1]:])
        new_parts.append(text[offsets[-1][1]:])

    # sanity check
    assert text == ''.join(orig_parts), "INTERNAL ERROR:\n    '%s'\nvs\n    '%s'" % (
        text, ''.join(orig_parts))

    splittext = ''.join(new_parts)

    # sanity
    assert len(text) == len(splittext), "INTERNAL ERROR"
    assert _normspace(text) == _normspace(splittext), "INTERNAL ERROR:\n    '%s'\nvs\n    '%s'" % (
        _normspace(text), _normspace(splittext))

    return splittext


def write_to_file(path, file_name, bio_list):

    # print(bio_list)
    del bio_list[0]  # 去掉文件编号
    bio_path = path + '\\' + file_name + '.bio'
    with open(bio_path, 'w', encoding='utf-8') as f:
        for item in bio_list:
            if item:
                f.write(item[2] + ' ' + item[3] + '\n')
            else:
                f.write('\n')


def get_filename_list(data_path):

    ann_list = []
    txt_list = []
    for filename in os.listdir(data_path):
        if filename[-4:] == '.ann':
            ann_list.append(filename[:-4])
        elif filename[-4:] == '.txt':
            txt_list.append(filename[:-4])
        else:
            continue
    intersection_set = [x for x in ann_list if x in set(txt_list)]
    # print(intersection_set)
    return intersection_set


def generate_bio(path):

    file_name_list = get_filename_list(path)
    for file_name in file_name_list:
        bio_list = txt_to_bio(path, file_name)
        write_to_file(path, file_name, bio_list)


if __name__ == '__main__':

    path = r'C:\Users\herunlong\Desktop\BIO相关\data'  # 存放.txt文件和.ann文件的文件夹
    generate_bio(path)
