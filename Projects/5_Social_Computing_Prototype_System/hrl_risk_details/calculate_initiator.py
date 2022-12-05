# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch
from duplicate import duplicate


def initialization(index_name, event_name, es):
    '''
    get start timestamp and end timestamp of the whole event
    used as global vars
    '''
    global first_timestamp
    global last_timestamp

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggs": {
            "min_timestamp": {
                "min": {
                    "field": "timestamp"
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)
    first_timestamp = response["aggregations"]["min_timestamp"]["value"]

    query_body_2 = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggs": {
            "max_timestamp": {
                "max": {
                    "field": "timestamp"
                }
            }
        }
    }
    response_2 = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body_2)
    last_timestamp = response_2["aggregations"]["max_timestamp"]["value"]


def get_origin_blog(index_name, event_name, es):

    query_body = {
        "size": 2000,
        "query": {
            "bool": {
                "must": [
                {
                    "range": {
                        "timestamp": {
                            "gte": first_timestamp,
                            "lte": last_timestamp
                        }
                    }
                },
                {
                    "term": {
                        "message_type": 1   # 原创贴
                    }
                }
                ]
            }
        },
        "sort": {
            "timestamp": {
                "order": "asc"
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)
    content = response['hits']['hits']

    input_for_duplicate = []
    for i in range(len(content)):
        d = dict()
        d['_id'] = i
        d['title'] = content[i]['_source']['keywords_dict']
        d['content'] = content[i]['_source']['text']
        d['timestamp'] = content[i]['_source']['timestamp']
        d['mid'] = content[i]['_source']['mid']  # necessary if blog is needed
        d['uid'] = content[i]['_source']['uid']
        input_for_duplicate.append(d)

    return input_for_duplicate


def get_initiator_uid(input_for_duplicate):
    '''
    initiator算法问张志豪
    duplicate算法问林浩
    '''

    # 文本去重
    duplicate_result = duplicate(input_for_duplicate)

    # 相似文本聚簇
    cluster = []
    for i in range(len(duplicate_result)):
        if duplicate_result[i]['duplicate'] == True:
            cluster.append(duplicate_result[i]['same_from'])

    d = dict()
    for key in cluster:
        d[key] = d.get(key, 0) + 1

    initiator_uid = 0
    if not d:
        return initiator_uid
    else:
        target_id = max(d, key = d.get)
        for item in input_for_duplicate:
            if item['_id'] == target_id:
                initiator_uid = item['uid']
                break
        return initiator_uid


def initiator_interface(index_name, event_name, es):

    initialization(index_name, event_name, es)

    # 获取微博原创帖子
    input_for_duplicate = get_origin_blog(index_name, event_name, es)
    # 计算事件推动者
    initiator_uid = get_initiator_uid(input_for_duplicate)

    # print initiator_uid
    return initiator_uid


if __name__ == '__main__':

    index_name = "weibo_data_text"
    event_name = "林丹"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    initiator_interface(index_name, event_name, es)
    et = time.time()

    print "running time: ", et - st