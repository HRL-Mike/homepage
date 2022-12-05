# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
from elasticsearch import Elasticsearch

from subevent_analysis_module_word_cloud import subevent_word_interface
from delete_obsolete_data import delete_interface

from global_utils import index_name_for_subevent_storage


def create_index_for_word_frequency(index_name, es):
    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        # 不设置则mapping, 则type使用默认值
        # 建议手动设置string的type值
        }
    }
    es.indices.create(index = index_name, body = create_body, ignore = 400)


def index_data_for_word_frequency(index_name, events, result_list, es):

    es.index(index = index_name, doc_type = events, id = timestamp_to_date(time.time()), body = {
        'events': events,
        'word_frequency_list': json.dumps(result_list),
        'date_of_computation': timestamp_to_date(time.time()),
        'timestamp_of_computation': int(time.time())
    })

    print 'index word frequency data successfully'


def timestamp_to_date(unix_time):
    '''
    convert unix timestamp to datetime
    '''
    format = '%Y-%m-%d'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def subevent_storage_interface(events, es):

    result_list = subevent_word_interface(events, es)

    # 创建索引
    create_index_for_word_frequency(index_name_for_subevent_storage, es)

    # 删除旧数据
    delete_interface(index_name_for_subevent_storage, events, es)

    # 索引数据
    index_data_for_word_frequency(index_name_for_subevent_storage, events, result_list, es)

    return None


if __name__ == '__main__':

    events = "台独_占中_港独"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    subevent_storage_interface(events, es)
    et = time.time()

    print "running time: ", et - st