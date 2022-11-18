# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
from elasticsearch import Elasticsearch

# global vars
from global_utils import index_name_for_curve
from global_utils import index_name_for_content
from global_utils import index_name_for_risk_point_curve
from global_utils import num_of_prediction


def get_length(index_name, event_name, es):
    print "44444"
    print index_name,event_name,es
    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)
    print response
    print("=============================================")
    print(response["hits"]["total"])
    return response["hits"]["total"]

def extract_curve_result(index_name, event_name, es):
    print "33333"
    evolution_and_key_user_result_list = []
    heat_and_emotion_result_list = []
    length = get_length(index_name, event_name, es)
    print length
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
            index = index_name,
            doc_type = event_name,
            body = query_body)

        # 获取风险点索引中的事件指数
        d1 = dict()

        d1["total_heat"] = response["hits"]["hits"][0]["_source"]["total_heat"]
        # d1["blog_heat"] = response["hits"]["hits"][0]["_source"]["blog_heat"]       # 修正为帖子总数
        d1["num_of_blog"] = response["hits"]["hits"][0]["_source"]["num_of_blog"]
        d1["num_of_baidu_news"] = response["hits"]["hits"][0]["_source"]["num_of_baidu_news"]
        d1["num_of_wechat_article"] = response["hits"]["hits"][0]["_source"]["num_of_wechat_article"]

        d1["negative_percent"] = response["hits"]["hits"][0]["_source"]["negative_percent"]

        d2 = dict()
        d2["num_of_news_source"] = response["hits"]["hits"][0]["_source"]["num_of_news_source"]
        d2["num_of_article_source"] = response["hits"]["hits"][0]["_source"]["num_of_article_source"]
        d2["num_of_blog_participant"] = response["hits"]["hits"][0]["_source"]["num_of_blog_participant"]

        heat_and_emotion_result_list.append(d1)
        evolution_and_key_user_result_list.append(d2)

    # 事件指数和阶段划分
    new_length = length + num_of_prediction
    event_index= []
    stage = []
    timestamp = []
    prediction = []
    for i in range(new_length):
        query_body_2 = {
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
        response_2 = es.search(
            index = index_name_for_risk_point_curve,
            doc_type = event_name,
            body = query_body_2)
        stage.append(response_2["hits"]["hits"][0]["_source"]["stage"])
        timestamp.append(response_2["hits"]["hits"][0]["_source"]["timestamp"])
        if i <= length-1:
            event_index.append(response_2["hits"]["hits"][0]["_source"]["event_index"])
        else:
            prediction.append(response_2["hits"]["hits"][0]["_source"]["event_index"])

    return evolution_and_key_user_result_list, heat_and_emotion_result_list, event_index, prediction, stage, timestamp


def extract_content_result(index_name, event_name, timestamp, es):

    result_list = []

    query_body = {
        "size": 30,  # 只返回30条
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "timestamp": timestamp
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)

    length = response["hits"]["total"]

    # 全都取出来
    for i in range(length):
        d = dict()
        d["datetime"] = response["hits"]["hits"][i]["_source"]["datetime"]
        d["uid"] = response["hits"]["hits"][i]["_source"]["uid"]
        d["text"] = response["hits"]["hits"][i]["_source"]["text"]
        d["num_of_comment"] = response["hits"]["hits"][i]["_source"]["num_of_comment"]
        d["num_of_forward"] = response["hits"]["hits"][i]["_source"]["num_of_forward"]
        d["user_name"] = response["hits"]["hits"][i]["_source"]["user_name"]

        d["total_count"] = response["hits"]["hits"][i]["_source"]["total_count"]
        result_list.append(d)

    # 根据评论转发总数排序
    length = len(result_list)
    while length > 0:
        for j in range(len(result_list) - 1):
            if result_list[j]["total_count"] < result_list[j + 1]["total_count"]:
                temp = result_list[j]
                result_list[j] = result_list[j + 1]
                result_list[j + 1] = temp
        length -= 1

    # 如果存储时某个时间戳是空列表，则取出时亦为空列表
    return result_list





def curve_output_for_frontend(event_name, es):


    evolution_and_key_user, heat_and_emotion, event_index, prediction, stage, timestamp = \
        extract_curve_result(index_name_for_curve, event_name, es)
    print "222222222"
    print evolution_and_key_user,heat_and_emotion
    d = dict()
    d["evolution_and_key_user"] = evolution_and_key_user
    d["heat_and_emotion"] = heat_and_emotion
    d['event_index'] = event_index
    d['stage'] = stage
    d['timestamp'] = timestamp
    d['prediction'] = prediction
    print d

    return json.dumps(d)


def content_output_for_frontend(event_name, timestamp, page_num, page_size, es):   # 每次请求现调用

    # 返回该时间戳对应的所有帖子
    content_result_list = extract_content_result(index_name_for_content, event_name, timestamp, es)

    # 分页
    start_from = (int(page_num) - 1) * int(page_size)
    end_at = int(start_from) + int(page_size)

    temp_list = content_result_list[start_from:end_at]

    result_list = []
    d = dict()
    d["total"] = len(content_result_list)
    d["data"] = temp_list
    result_list.append(d)

    return result_list


if __name__ == '__main__':

    page_num = 1
    page_size = 5
    es = Elasticsearch(['219.224.134.226:9207'])
    event_name = "林丹"

    st = time.time()
    a = curve_output_for_frontend(event_name, es)
    c = content_output_for_frontend(event_name, 1524441600, page_num, page_size, es)
    et = time.time()
    print "running time: ", et - st