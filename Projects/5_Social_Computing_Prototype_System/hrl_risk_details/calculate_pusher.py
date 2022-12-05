# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch


def initialization(index_name, event_name, es):
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



def get_blog(index_name, event_name, es):
    '''
    返回起止时间内所有mid
    '''
    query_body = {
        "size": 0,
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
                        "message_type": 1
                    }
                }
                ]
            }
        },
        "aggs": {
            "mid": {
                "terms": {
                    "field": "mid",
                    "size": 100000
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)
    buckets = response["aggregations"]["mid"]["buckets"]

    mid_list = []
    for i in range(len(buckets)):
        mid_list.append(buckets[i]['key'])

    return mid_list


def get_target_mid(mid_list, index_name, event_name, es):

    num_list = []
    for mid in mid_list:
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "terms":{
                                        "message_type": [2, 3]
                                    }
                                },
                                {
                                    "term": {
                                        "root_mid": mid
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }
        response = es.search(
            index = index_name,
            doc_type = event_name,
            body = query_body)
        num = response["hits"]["total"]
        num_list.append(num)
    index = num_list.index(max(num_list))
    target_mid = mid_list[index]

    return target_mid


def get_pusher_uid(target_mid, index_name, event_name, es):

    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "mid": target_mid
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)
    content = response['hits']['hits']
    target_uid = content[0]['_source']['uid']

    return target_uid


def pusher_interface(index_name, event_name, es):

    initialization(index_name, event_name, es)

    # 获取各时间段内的mid
    mid_list = get_blog(index_name, event_name, es)
    # 找出所有mid中评论转发数最大的
    target_mid = get_target_mid(mid_list, index_name, event_name, es)
    # 获取这条微博对应的uid
    pusher_uid = get_pusher_uid(target_mid, index_name, event_name, es)

    # print pusher_uid
    return pusher_uid


if __name__ == '__main__':

    index_name = "flow_text_data"
    event_name = "gangdu"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    pusher_interface(index_name, event_name, es)
    et = time.time()

    print "running time: ", et - st