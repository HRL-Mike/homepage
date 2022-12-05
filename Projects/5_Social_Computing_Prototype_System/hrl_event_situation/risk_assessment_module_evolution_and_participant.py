# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch
from risk_assessment_module_heat_and_emotion import heat_and_emotion_interface

# global variables
from global_utils import time_slice
from global_utils import index_name_for_baidu_news
from global_utils import index_name_for_wechat_data
from global_utils import coefficient_for_risk_index


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


def get_num_of_participant(index_name, event_name, es):

    query_body = {
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
                    "participant": {
                        "terms": {
                            "field": "uid",
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

    # get num of participants in each time interval
    num_of_participant = []
    buckets = response["aggregations"]["time_slice"]["buckets"]
    for i in range(len(buckets)):
        num = len(buckets[i]["participant"]["buckets"])
        num_of_participant.append(num)

    return num_of_participant


def get_sensitive_risk(index_name, event_name, es):
    '''
    calculate values for risk evolution curve
    include risk index, heat risk, emotion risk and sensitive risk

    :param event_name: name of event
    :param heat_index_list: a list containing heat index for each interval
    :param negative_percentage: a list containing negative percent for each interval

    :return: result_list_for_frontend: a list containing risk index, heat risk,
             emotion risk and sensitive risk for each interval
    '''
    # get avg sensitive value in each interval
    query_body = {
        "size": 0,
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
                    "sensitive": {
                        "avg": {
                            "field": "sensitive"
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

    sensitive_value_list = []
    buckets = response["aggregations"]["time_slice"]["buckets"]
    for i in range(len(buckets)):
        if buckets[i]["sensitive"]["value"] != None:
            sensitive_value_list.append(round(buckets[i]["sensitive"]["value"], 2))
        else:
            sensitive_value_list.append(0.0)

    return sensitive_value_list


def source_count(index_name, event_name, es, field_name):
    '''
        统计新闻源和公众号的数量
    '''
    start_index = 0
    end_index = 0
    temp_list = []

    query_body = {
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
                    "source_count": {
                        "terms": {
                            "field": field_name,
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
    buckets = response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        d = dict()
        d["timestamp"] = buckets[i]["key"]
        d["num_of_source"] = len(buckets[i]["source_count"]["buckets"])
        temp_list.append(d)

    # 截取有效内容
    for i in range(len(temp_list)):
        if temp_list[i]["timestamp"] == start_timestamp:
            start_index = i
        if temp_list[i]["timestamp"] == end_timestamp:
            end_index = i

    end_index = end_index + 1  # 截取区间左闭右开
    news_source_count = temp_list[start_index:end_index]

    return news_source_count


def calculate_risk_index(total_heat, baidu_news_count, wechat_article_count, sensitive_result,
                         negative_percent, num_of_uid, coefficient):
    '''
        计算风险指数，线性算子
    '''
    result_list = []
    for i in range(len(total_heat)):
        risk_index = total_heat[i] * coefficient[0] + negative_percent[i] * coefficient[1] + \
                     sensitive_result[i] * coefficient[2] + num_of_uid[i] * coefficient[3] + \
                     baidu_news_count[i]["num_of_source"] * coefficient[4] + \
                     wechat_article_count[i]["num_of_source"] * coefficient[5]
        result_list.append(int(round(risk_index)))

    return result_list


def organize_heat_curve_result(total_heat, heat_result, baidu_news_count, wechat_article_count):
    result_list = []

    for i in range(len(total_heat)):
        d = dict()
        d["total_heat"] = total_heat[i]
        d["blog_heat"] = heat_result[i]["heat"]
        d["blog_origin"] = heat_result[i]["origin"]
        d["blog_comment"] =  heat_result[i]["comment"]
        d["blog_forward"] = heat_result[i]["forward"]
        d["num_of_baidu_news"] = baidu_news_count[i]["num_of_baidu_news"]
        d["num_of_wechat_article"] = wechat_article_count[i]["num_of_wechat_article"]
        result_list.append(d)

    return result_list


def organize_participation_curve_result(num_of_uid, baidu_source_count, wechat_source_count):
    result_list = []

    for i in range(len(num_of_uid)):
        d = dict()
        d["num_of_blog_participant"] = num_of_uid[i]
        d["num_of_news_source"] = baidu_source_count[i]["num_of_source"]
        d["num_of_article_source"] = wechat_source_count[i]["num_of_source"]
        result_list.append(d)

    return result_list


def organize_risk_curve_result(risk_index, cumulative_risk_index, total_heat, sensitive_result, negative_percentage):
    result_list = []

    for i in range(len(risk_index)):
        d = dict()
        d["risk_index"] = risk_index[i]
        d["cumulative_risk_index"] = cumulative_risk_index[i]
        d["heat_risk"] = total_heat[i]
        d["emotion_risk"] = negative_percentage[i]
        d["sensitive_risk"] = sensitive_result[i]
        result_list.append(d)

    return result_list


def get_cumulated_risk_index(risk_index):
    '''
        计算风险指数累计值
    '''
    total = 0
    cumulated_risk_index = []

    for i in range(len(risk_index)):
        total = total + risk_index[i]
        cumulated_risk_index.append(total)

    return cumulated_risk_index


def curve_interface(index_name, event_name, es):

    field_name_baidu = "news_source"
    field_name_wechat = "source"

    initialization(index_name, event_name, es)

    # invoke heat_and_emotion_interface
    timestamp_list, heat_result, emotion_result, negative_percentage, \
    baidu_news_count, wechat_article_count, total_heat = heat_and_emotion_interface(index_name, event_name, es)

    # get participant
    baidu_source_count = source_count(index_name_for_baidu_news, event_name, es, field_name_baidu)
    wechat_source_count = source_count(index_name_for_wechat_data, event_name, es, field_name_wechat)
    num_of_uid = get_num_of_participant(index_name, event_name, es)  # 关键用户识别被微博用户数量取代了

    # get risk index and cumulative risk index
    sensitive_result = get_sensitive_risk(index_name, event_name, es)
    risk_index = calculate_risk_index(total_heat, baidu_source_count, wechat_source_count, sensitive_result,
                                      negative_percentage, num_of_uid, coefficient_for_risk_index)
    cumulative_risk_index = get_cumulated_risk_index(risk_index)

    # 整理结果
    heat_curve_result = organize_heat_curve_result(total_heat, heat_result, baidu_news_count, wechat_article_count)
    participation_curve_result = organize_participation_curve_result(num_of_uid, baidu_source_count, wechat_source_count)
    risk_curve_result = organize_risk_curve_result(risk_index, cumulative_risk_index, total_heat,
                                                   sensitive_result, negative_percentage)   # 风险指数被修正为事件指数

    return timestamp_list, heat_curve_result, emotion_result, \
           participation_curve_result, risk_curve_result


if __name__ == '__main__':

    index_name = "weibo_data_text"
    event_name = "港独"  # 索引里没有该事件的数据则报错(列表索引越界)
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    curve_interface(index_name, event_name, es)
    et = time.time()

    print "4 curves running time: ", et - st