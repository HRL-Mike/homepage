# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
from elasticsearch import Elasticsearch

from global_utils import index_name_for_subevent_storage


def extract_word_frequency_result(index_name, date, events, es):

    top_50_list = []
    response = es.get(  # 根据id查询具体的某一条记录
        index = index_name,
        doc_type = events,
        id = date)

    if response == []:
        return top_50_list
    else:
        word_frequency_list = json.loads(response["_source"]['word_frequency_list'])
        top_50_list = word_frequency_list[0:50]   # 计算了300个结果，但只取前50
    return top_50_list


def subevent_word_frequency_interface(events, date, es):

    word_frequency_list = extract_word_frequency_result(index_name_for_subevent_storage, date, events, es)

    d = dict()
    d['subevent_word_frequency'] = word_frequency_list
    # print d

    return json.dumps(d)


if __name__ == '__main__':

    events = "台独_占中_港独"
    date = '2019-03-12'
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    subevent_word_frequency_interface(events, date, es)
    et = time.time()

    print "running time: ", et - st