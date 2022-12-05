# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch

from global_utils import time_slice
from global_utils import index_name_for_uid_mapping_table
from global_utils import event_name_for_uid_mapping_table


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
                    "interval": time_slice,  # global var
                    "min_doc_count": 0
                }
            }
        }
    }
    response = es.search(
        index=index_name,
        doc_type=event_name,
        body=query_body)

    start_timestamp = response["aggregations"]["time_slice"]["buckets"][0]["key"]
    end_timestamp = response["aggregations"]["time_slice"]["buckets"][-1]["key"]


def prepare_risk_details(index_name, event_name, es):
    '''
    generate the blog for risk details
    take 30 mid and count their comment and forward

    :param event_name: name of event

    :return: hot_post_list: a list containing hot posts in each interval
                            their num of comment and forward have been identified
    '''
    query_body = {   # 返回24小时内全部mid
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggs": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                },
                "aggs": {
                    "hot_blogs": {
                        "terms": {
                            "field": "mid",
                            "size": 100000
                        }
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)

    # get hot blog and count their num of comment and forward
    hot_post_list = get_hot_posts(index_name, event_name, response, es)

    # bubble sort, desc
    hot_post_list_in_order = rank(hot_post_list)

    return hot_post_list_in_order


def get_hot_posts(index_name, event_name, response, es):

    result_list = []
    #print("response!!!!",response)
    buckets = response["aggregations"]["time_slice"]["buckets"]   # len = 137

    for i in range(len(buckets)):
        temp = []
        for j in range(len(buckets[i]["hot_blogs"]["buckets"])):
            d = dict()
            num_of_comment = query_for_hot_posts(index_name, event_name, 2, buckets[i]["hot_blogs"] \
                                                ["buckets"][j]["key"], buckets[i]["key"], es)
            num_of_forward = query_for_hot_posts(index_name, event_name, 3, buckets[i]["hot_blogs"] \
                                                ["buckets"][j]["key"], buckets[i]["key"], es)
            total = num_of_comment + num_of_forward
            mid = buckets[i]["hot_blogs"]["buckets"][j]["key"]
            d["mid"] = mid
            d["total_count"] = total
            d["comment"] = num_of_comment
            d["forward"] = num_of_forward
            d["timestamp"] = buckets[i]["key"]   # 同一天的帖子时间戳一致
            d["type"] = event_name
            temp.append(d)
        result_list.append(temp)

    return result_list


def query_for_hot_posts(index_name, event_name, message_type, mid, start_timestamp, es):

    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "message_type": message_type
                                }
                            },
                            {
                                "term": {
                                    "root_mid": mid
                                }
                            },
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": start_timestamp,
                                        "lte": start_timestamp + time_slice
                                    }
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

    return response["hits"]["total"]


def rank(hot_post_list):

    new_hot_post_list = []

    for i in range(len(hot_post_list)):
        length = len(hot_post_list[i])
        while length > 0:
            for j in range(length - 1):
                if hot_post_list[i][j]["total_count"] < hot_post_list[i][j + 1]["total_count"]:
                    temp = hot_post_list[i][j]
                    hot_post_list[i][j] = hot_post_list[i][j + 1]
                    hot_post_list[i][j + 1] = temp
            length -= 1

        new_hot_post_list.append(hot_post_list[i][0:30])

    return new_hot_post_list


def construct_risk_details(index_name, event_name, content_result, es):
    '''
    add some fields for risk details
    include uid, key, text and datetime

    :param event_name: name of event
    :param content_result: a list that contains incomplete content result

    :return: content_result
    '''
    for i in range(len(content_result)):
        if content_result[i] == []:
            continue
        else:
            for j in range(len(content_result[i])):
                query_body = {
                    "query": {
                        "filtered": {
                            "filter": {
                                "term": {
                                    "mid": content_result[i][j]["mid"]
                                }
                            }
                        }
                    }
                }
                response = es.search(
                    index = index_name,
                    doc_type = event_name,
                    body = query_body)   # 9209没有didi 9207才有

                # time.sleep(0)
                # print i, j
                # print response["hits"]["hits"]

                if response["hits"]["hits"] == []:
                    continue

                content_result[i][j]["uid"] = response["hits"]["hits"][0]["_source"]["uid"]
                content_result[i][j]["text"] = response["hits"]["hits"][0]["_source"]["text"]
                content_result[i][j]["datetime"] = timestamp_to_date_with_second(response["hits"]["hits"] \
                                                                     [0]["_source"]["timestamp"])
                query_body_2 = {
                    "query": {
                        "filtered": {
                            "filter": {
                                "term": {
                                    "uid": content_result[i][j]["uid"]
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
                    content_result[i][j]["user_name"] = None
                else:
                    content_result[i][j]["user_name"] = response_2["hits"]["hits"][0]["_source"]["name"]

    return content_result


def timestamp_to_date_with_second(unix_time):
    '''
    convert unix timestamp to datetime
    '''
    format = '%Y-%m-%d %H:%M:%S'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def details_interface(index_name, event_name, es):

    initialization(index_name, event_name, es)

    # 按评论转发对帖子进行排序，返回前30条帖子的mid和相关信息
    hot_post_list = prepare_risk_details(index_name, event_name, es)
    # 根据mid补全帖子内容
    table_for_risk_details = construct_risk_details(index_name, event_name, hot_post_list, es)

    return table_for_risk_details


if __name__ == '__main__':

    index_name = "weibo_data_text"
    event_name = "滴滴"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    a = details_interface(index_name, event_name, es)
    et = time.time()

    print "details running time: ", et - st