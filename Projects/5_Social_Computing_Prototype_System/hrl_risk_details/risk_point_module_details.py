# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from elasticsearch import Elasticsearch

from global_utils import time_slice
from global_utils import threshold_for_sensitive_value
from global_utils import index_name_for_uid_mapping_table
from global_utils import event_name_for_uid_mapping_table
from global_utils import threshold_for_num_of_sensitive_blog
from global_utils import threshold_for_negative_emotion

from risk_point_module_micro import micro_interface_for_details



def initialization(index_name, event_name, es):
    '''
    get start timestamp and end timestamp of the whole event
    used as global vars
    '''
    global start_timestamp
    global end_timestamp

    query_body = {
        "size": 0,
        "query": {
           "match_all": {}
        },
        "aggregations": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)

    start_timestamp = response["aggregations"]["time_slice"]["buckets"][0]["key"]
    end_timestamp = response["aggregations"]["time_slice"]["buckets"][-1]["key"]
    length = len(response["aggregations"]["time_slice"]["buckets"])
    return length

    # print start_timestamp  1523923200
    # print end_timestamp   1526860800


def change_format(input_list, record):
    '''
    改变record的数据结构
    从记录各uid在哪个时间段里出现转化到各时间段里有哪些uid出现
    '''
    uid_list = []
    timestamp = start_timestamp
    duration = len(input_list)

    for a in range(duration):
        uid_list.append(list())

    # 格式转换 现在记录每天有哪些uid出现
    for i in range(duration):
        for j in range(len(record)):  # uid_list长度
            for t in record[j]["timestamp"]:
                if t == timestamp:
                    uid_list[i].append(record[j]["uid"])
        timestamp = timestamp + time_slice

    return uid_list


def get_post(uid, type, index_name, event_name, es):
    details = []
    timestamp = start_timestamp

    for i in range(len(uid)):
        if uid[i] == []:
            timestamp = timestamp + time_slice
            continue
        else:
            for j in range(len(uid[i])):
                query_body = {
                    "query": {
                        "bool": {
                            "must": [
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": timestamp,
                                        "lt": timestamp + time_slice
                                    }
                                }
                            },
                            {
                                "term": {
                                    "uid": uid[i][j]
                                }
                            }
                            ]
                        }
                    }
                }
                response = es.search(
                    index = index_name,
                    doc_type = event_name,
                    body = query_body)
                content = response["hits"]["hits"]

                if content == []:
                    continue
                else:
                    d = dict()
                    d["uid"] = content[0]["_source"]["uid"]
                    d["text"] = content[0]["_source"]["text"]
                    d["mid"] = content[0]["_source"]["mid"]
                    d["keywords_string"] = content[0]["_source"]["keywords_string"]
                    d["real_timestamp"] = content[0]["_source"]["timestamp"]
                    d["sensitive"] = content[0]["_source"]["sensitive"]
                    d["timestamp"] = timestamp
                    if type == 1:
                        d["type"] = "key_media"
                    if type == 2:
                        d["type"] = "bigv"
                    if type == 3:
                        d["type"] = "sensitive_user"
                    details.append(d)
            timestamp = timestamp + time_slice

    # 计算评论转发数
    calculate_num_of_comment_and_forward(details, index_name, event_name, es)

    # uid映射用户名
    for k in range(len(details)):
        query_body_2 = {
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            "uid": details[k]["uid"]
                        }
                    }
                }
            }
        }
        response_2 = es.search(
            index = index_name_for_uid_mapping_table,
            doc_type = event_name_for_uid_mapping_table,
            body = query_body_2)

        if response_2["hits"]["hits"] == []:
            details[k]["user_name"] = None
        else:
            details[k]["user_name"] = response_2["hits"]["hits"][0]["_source"]["name"]

    return details


def get_sensitive_post(duration, num_of_blog, index_name, event_name, es):
    sensitive_post = []
    timestamp_list = []

    for m in range(len(num_of_blog)):
        if num_of_blog[m] < threshold_for_num_of_sensitive_blog:
            num_of_blog[m] = 0

    temp = start_timestamp
    for n in range(len(num_of_blog)):
        if num_of_blog[n] == 0:
            timestamp_list.append(0)
            temp = temp + time_slice
        else:
            timestamp_list.append(temp)
            temp = temp + time_slice

    timestamp = start_timestamp
    for i in range(duration):
        if timestamp_list[i] == 0:
            timestamp = timestamp + time_slice
            continue
        else:
            query_body = {
                "size": 1000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "sensitive": {
                                        "gte": threshold_for_sensitive_value
                                    }
                                }
                            },
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": timestamp,
                                        "lt": timestamp + time_slice
                                    }
                                }
                            }]
                    }
                }
            }
            response = es.search(
                index = index_name,
                doc_type = event_name,
                body = query_body)
            content = response["hits"]["hits"]

            for i in range(len(content)):
                d = dict()
                d["uid"] = content[i]["_source"]["uid"]
                d["text"] = content[i]["_source"]["text"]
                d["mid"] = content[i]["_source"]["mid"]
                d["keywords_string"] = content[i]["_source"]["keywords_string"]
                d["real_timestamp"] = content[i]["_source"]["timestamp"]
                d["sensitive"] = content[i]["_source"]["sensitive"]
                d["timestamp"] = timestamp
                d["type"] = "sensitive_blog"
                sensitive_post.append(d)
            timestamp = timestamp + time_slice

    # 计算评论转发数
    calculate_num_of_comment_and_forward(sensitive_post, index_name, event_name, es)

    # uid映射用户名
    for k in range(len(sensitive_post)):
        query_body_2 = {
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            "uid": sensitive_post[k]["uid"]
                        }
                    }
                }
            }
        }
        response_2 = es.search(
            index = index_name_for_uid_mapping_table,
            doc_type = event_name_for_uid_mapping_table,
            body = query_body_2)

        if response_2["hits"]["hits"] == []:
            sensitive_post[k]["user_name"] = None
        else:
            sensitive_post[k]["user_name"] = response_2["hits"]["hits"][0]["_source"]["name"]

    return sensitive_post


def get_negative_post(duration, negative_ratio, index_name, event_name, es):
    negative_post = []
    timestamp_list = []

    for m in range(len(negative_ratio)):
        if negative_ratio[m] < threshold_for_negative_emotion:
            negative_ratio[m] = 0

    temp = start_timestamp
    for n in range(len(negative_ratio)):
        if negative_ratio[n] == 0:
            timestamp_list.append(0)
            temp = temp + time_slice
        else:
            timestamp_list.append(temp)
            temp = temp + time_slice

    timestamp = start_timestamp
    for i in range(duration):
        if timestamp_list[i] == 0:
            timestamp = timestamp + time_slice
            continue
        else:
            query_body = {
                "size": 1000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "sentiment": 3
                                }
                            },
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": timestamp,
                                        "lt": timestamp + time_slice
                                    }
                                }
                            }]
                    }
                }
            }
            response = es.search(
                index=index_name,
                doc_type=event_name,
                body=query_body)
            content = response["hits"]["hits"]

            for i in range(len(content)):
                d = dict()
                d["uid"] = content[i]["_source"]["uid"]
                d["text"] = content[i]["_source"]["text"]
                d["mid"] = content[i]["_source"]["mid"]
                d["keywords_string"] = content[i]["_source"]["keywords_string"]
                d["real_timestamp"] = content[i]["_source"]["timestamp"]
                d["sensitive"] = content[i]["_source"]["sensitive"]
                d["timestamp"] = timestamp
                d["type"] = "negative_blog"
                negative_post.append(d)
            timestamp = timestamp + time_slice

    # 计算评论转发数
    calculate_num_of_comment_and_forward(negative_post, index_name, event_name, es)

    # uid映射用户名
    for k in range(len(negative_post)):
        query_body_2 = {
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            "uid": negative_post[k]["uid"]
                        }
                    }
                }
            }
        }
        response_2 = es.search(
            index = index_name_for_uid_mapping_table,
            doc_type = event_name_for_uid_mapping_table,
            body = query_body_2)

        if response_2["hits"]["hits"] == []:
            negative_post[k]["user_name"] = None
        else:
            negative_post[k]["user_name"] = response_2["hits"]["hits"][0]["_source"]["name"]

    return negative_post


def calculate_num_of_comment_and_forward(list, index_name, event_name, es):

    for i in range(len(list)):
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "terms": {
                                        "message_type": [2, 3]
                                    }
                                },
                                {
                                    "term": {
                                        "root_mid": list[i]['mid']
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
        list[i]['num_of_comment_and_forward'] = response["hits"]["total"]


def details_interface(index_name, event_name, es):

    duration = initialization(index_name, event_name, es)

    # 调用接口，获取与代表内容相关的中间结果
    media_list, media_record, bigv_list, bigv_record, sensitive_user_list, sensitive_user_record, \
    num_of_sensitive_blog , negative_emotion_ratio = micro_interface_for_details(index_name, event_name, es)

    # record格式转化
    media_uid_list = change_format(media_list, media_record)
    bigv_uid_list = change_format(bigv_list, bigv_record)
    sensitive_user_uid_list = change_format(sensitive_user_list, sensitive_user_record)

    # 获取帖子内容(媒体，大V，敏感用户)，存入列表
    media_post = get_post(media_uid_list, 1, index_name, event_name, es)
    bigv_post = get_post(bigv_uid_list, 2, index_name, event_name, es)
    sensitive_user_post = get_post(sensitive_user_uid_list, 3, index_name, event_name, es)

    # 获取敏感帖子内容
    sensitive_blog = get_sensitive_post(duration, num_of_sensitive_blog, index_name, event_name, es)
    # 获取消极帖子内容
    negative_blog = get_negative_post(duration, negative_emotion_ratio, index_name, event_name, es)

    return media_post, bigv_post, sensitive_user_post, sensitive_blog, negative_blog


if __name__ == '__main__':

    index_name = "flow_text_data"
    event_name = "gangdu"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    details_interface(index_name, event_name, es)
    et = time.time()

    print "running time: ", et - st