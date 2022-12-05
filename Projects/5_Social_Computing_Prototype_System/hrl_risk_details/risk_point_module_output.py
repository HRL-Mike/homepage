# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
from elasticsearch import Elasticsearch

from global_utils import index_name_for_risk_point_curve
from global_utils import index_name_for_risk_point_content


def extract_curve_result(index_name, event_name, es):

    curve_result_list = []

    length = get_length(index_name, event_name, es)

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

        d = dict()
        d["timestamp"] = response["hits"]["hits"][0]["_source"]["timestamp"]

        d["heat_risk"] = response["hits"]["hits"][0]["_source"]["heat_risk"]
        d["trend_risk"] = response["hits"]["hits"][0]["_source"]["trend_risk"]
        d["emotion_risk"] = response["hits"]["hits"][0]["_source"]["emotion_risk"]
        d["sensitive_risk"] = response["hits"]["hits"][0]["_source"]["sensitive_risk"]
        d["event_index"] = response["hits"]["hits"][0]["_source"]["event_index"]
        # d['risk_of_event_index'] = response["hits"]["hits"][0]["_source"]["risk_of_event_index"]
        d['risk_level'] = response["hits"]["hits"][0]["_source"]["risk_level"]

        risk_point = response["hits"]["hits"][0]["_source"]["risk_point"]
        if risk_point == None:
            d["risk_point"] = None
        else:
            classified_risk_point = classify_risk_point(risk_point)
            d["risk_point"] = classified_risk_point
        curve_result_list.append(d)

    return curve_result_list


def classify_risk_point(risk_point):
    heat_risk_list = []
    trend_risk_list = []
    emotion_risk_list = []
    sensitive_risk_list = []
    result_list = []
    risk_point_list = json.loads(risk_point)

    for item in risk_point_list:
        risk_type = str(item[0])
        if risk_type == "type_A":
            heat_risk_list.append(item)
        if risk_type == "type_B":
            heat_risk_list.append(item)
        if risk_type == "type_C":
            heat_risk_list.append(item)

        if risk_type == "type_D":
            trend_risk_list.append(item)
        if risk_type == "type_E":
            trend_risk_list.append(item)
        if risk_type == "type_F":
            trend_risk_list.append(item)
        if risk_type == "type_G":
            trend_risk_list.append(item)

        if risk_type == "type_H":
            emotion_risk_list.append(item)
        if risk_type == "type_I":
            emotion_risk_list.append(item)
        if risk_type == "type_J":
            emotion_risk_list.append(item)

        if risk_type == "type_K":
            sensitive_risk_list.append(item)
        if risk_type == "type_L":
            sensitive_risk_list.append(item)
        if risk_type == "type_M":
            sensitive_risk_list.append(item)

    d = dict()
    if heat_risk_list:
        d["heat_risk_point"] = heat_risk_list
    if trend_risk_list:
        d["trend_risk_point"] = trend_risk_list
    if emotion_risk_list:
        d["emotion_risk_point"] = emotion_risk_list
    if sensitive_risk_list:
        d["sensitive_risk_point"] = sensitive_risk_list
    result_list.append(d)
    return result_list


def get_length(index_name, event_name, es):
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

    return response["hits"]["total"]


def extract_content_result(index_name, event_name, timestamp, curve_name, es, page_num, page_size):
    result_list = []

    emotion_risk_post, sensitive_risk_post = extract_all_blog(index_name, event_name, timestamp, es)

    emotion_risk_post = sort(emotion_risk_post)
    sensitive_risk_post = sort(sensitive_risk_post)

    if curve_name == "emotion_risk":
        # 分页
        start_from = (int(page_num) - 1) * int(page_size)
        end_at = int(start_from) + int(page_size)
        temp_list = emotion_risk_post[start_from:end_at]

        d = dict()
        d["total"] = len(emotion_risk_post)
        d["data"] = temp_list
        result_list.append(d)
        return result_list

    elif curve_name == "sensitive_risk":
        # 分页
        start_from = (int(page_num) - 1) * int(page_size)
        end_at = int(start_from) + int(page_size)
        temp_list = sensitive_risk_post[start_from:end_at]

        d = dict()
        d["total"] = len(sensitive_risk_post)
        d["data"] = temp_list
        result_list.append(d)
        return result_list
    else:
        return result_list


def extract_all_blog(index_name, event_name, timestamp, es):
    emotion_risk_post = []
    sensitive_risk_post = []

    query_body = {
        "size": 1000,
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
    content = response["hits"]["hits"]

    for i in range(len(content)):
        # print content[i]["_source"]["num_of_comment_and_forward"]
        if content[i]["_source"]["type"] == "sensitive_blog":
            d = dict()
            d["text"] = content[i]["_source"]["text"]
            d["sensitive"] = content[i]["_source"]["sensitive"]
            d["user_name"] = content[i]["_source"]["user_name"]
            d['num_of_comment_and_forward'] = content[i]["_source"]["num_of_comment_and_forward"]
            d['uid'] = content[i]["_source"]["uid"]
            d["date"] = content[i]["_source"]["date"]
            d["risk_type"] = "高敏感值帖子"
            emotion_risk_post.append(d)
        elif content[i]["_source"]["type"] == "negative_blog":
            d = dict()
            d["text"] = content[i]["_source"]["text"]
            d["sensitive"] = content[i]["_source"]["sensitive"]
            d["user_name"] = content[i]["_source"]["user_name"]
            d['num_of_comment_and_forward'] = content[i]["_source"]["num_of_comment_and_forward"]
            d['uid'] = content[i]["_source"]["uid"]
            d["date"] = content[i]["_source"]["date"]
            d["risk_type"] = "负面情绪帖子"
            emotion_risk_post.append(d)
        elif content[i]["_source"]["type"] == "sensitive_user":
            d = dict()
            d["text"] = content[i]["_source"]["text"]
            d["sensitive"] = content[i]["_source"]["sensitive"]
            d["user_name"] = content[i]["_source"]["user_name"]
            d['num_of_comment_and_forward'] = content[i]["_source"]["num_of_comment_and_forward"]
            d['uid'] = content[i]["_source"]["uid"]
            d["date"] = content[i]["_source"]["date"]
            d["risk_type"] = "敏感用户发帖"
            sensitive_risk_post.append(d)
        elif content[i]["_source"]["type"] == "bigv":
            d = dict()
            d["text"] = content[i]["_source"]["text"]
            d["sensitive"] = content[i]["_source"]["sensitive"]
            d["user_name"] = content[i]["_source"]["user_name"]
            d['num_of_comment_and_forward'] = content[i]["_source"]["num_of_comment_and_forward"]
            d['uid'] = content[i]["_source"]["uid"]
            d["date"] = content[i]["_source"]["date"]
            d["risk_type"] = "大V发帖"
            sensitive_risk_post.append(d)
        elif content[i]["_source"]["type"] == "key_media":
            d = dict()
            d["text"] = content[i]["_source"]["text"]
            d["sensitive"] = content[i]["_source"]["sensitive"]
            d["user_name"] = content[i]["_source"]["user_name"]
            d['num_of_comment_and_forward'] = content[i]["_source"]["num_of_comment_and_forward"]
            d['uid'] = content[i]["_source"]["uid"]
            d["date"] = content[i]["_source"]["date"]
            d["risk_type"] = "关键媒体发帖"
            sensitive_risk_post.append(d)
        else:
            print "unknown type occurs"
            continue

    return emotion_risk_post, sensitive_risk_post


def sort(post_list):
    length = len(post_list)

    while length > 0:
        for i in range(length-1):
            if post_list[i]["num_of_comment_and_forward"] < post_list[i+1]["num_of_comment_and_forward"]:
                temp = post_list[i]
                post_list[i] = post_list[i+1]
                post_list[i+1] = temp
        length = length - 1
    return post_list


def curve_output_interface(event_name, es):

    curve_result_list = extract_curve_result(index_name_for_risk_point_curve, event_name, es)

    d = dict()
    d["event_index_and_4_risks"] = curve_result_list
    # print d

    return json.dumps(d)


def content_output_interface(event_name, timestamp, curve_name, page_num, page_size, es):

    content_result_list = extract_content_result(index_name_for_risk_point_content, event_name,
                                                 timestamp, curve_name, es, page_num, page_size)

    d = dict()
    d["risk_point_details"] = content_result_list
    # print d

    return json.dumps(d)


if __name__ == '__main__':

    page_num = 1
    page_size = 5
    event_name = "gangdu"
    curve_name = "sensitive_risk"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    a = curve_output_interface(event_name, es)
    b = content_output_interface(event_name, 1525651200, curve_name, page_num, page_size, es)  #传入时间戳和曲线名字
    et = time.time()

    print "risk point output time: ", et - st