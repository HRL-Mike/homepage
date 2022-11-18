# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
from elasticsearch import Elasticsearch

from global_utils import index_name_for_risk_point_curve
from global_utils import num_of_prediction


def get_max_duration(event_list, es):

    max_timestamp_list = []
    for item in event_list:
        query_body ={
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
        response = es.search(
            index = index_name_for_risk_point_curve,
            doc_type = item,
            body = query_body)
        max_timestamp_list.append(response["aggregations"]["max_timestamp"]["value"])
    index = max_timestamp_list.index(max(max_timestamp_list))
    duration = get_length(event_list[index], es)

    return duration


def construct_event_index(event_list, duration, es):

    event_index = []
    # 获取各事件的事件指数
    for item in event_list:
        length = get_length(item, es) - num_of_prediction
        temp = []
        for i in range(length):
            query_body = {
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {
                                "sequence_number": i
                            }
                        }
                    }
                }
            }
            response = es.search(
                index = index_name_for_risk_point_curve,
                doc_type = item,
                body = query_body)
            temp.append(response["hits"]["hits"][0]["_source"]["event_index"])
        while len(temp) < duration:  # 补0
            temp.append(0)
        event_index.append(temp)

    event_index_result = []
    for j in range(len(event_list)):
        d = dict()
        d['event_name'] = event_list[j]
        d['index_list'] = event_index[j]
        event_index_result.append(d)

    return event_index_result


def get_length(event_name, es):
    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index = index_name_for_risk_point_curve,
        doc_type = event_name,
        body = query_body)

    return response["hits"]["total"]


def subevent_index_interface(events, es):

    # 获取事件列表
    event_list = events.split('_')
    # 计算几个事件中的最大持续时间(包含了预测的事件指数)
    max_duration = get_max_duration(event_list, es)
    # 真实的持续时间 = 最大持续时间 - 预测的天数
    duration = max_duration - num_of_prediction  # 索引中包含预测数据
    # 提取个事件的事件指数，不足补0
    event_index_result = construct_event_index(event_list, duration, es)

    d = dict()
    d['subevent_index'] = event_index_result
    # print d
    return json.dumps(d)


if __name__ == '__main__':

    events = "taidu_zhanzhong"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    subevent_index_interface(events, es)
    et = time.time()

    print "running time: ", et - st