# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch

# global variables
from global_utils import time_slice
from global_utils import coefficient_for_heat_index
from global_utils import coefficient_for_total_heat
from global_utils import index_name_for_baidu_news
from global_utils import index_name_for_wechat_data


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
        index = index_name,
        doc_type = event_name,
        body = query_body)

    start_timestamp = response["aggregations"]["time_slice"]["buckets"][0]["key"]
    end_timestamp = response["aggregations"]["time_slice"]["buckets"][-1]["key"]
    # print start_timestamp  1523923200
    # print end_timestamp   1526860800


def blog_heat_curve(index_name, event_name, es):

    field_name = "message_type"
    # get num of origin, comment & forward in each interval
    origin_response = query(index_name, event_name, field_name, 1, es)
    comment_response = query(index_name, event_name, field_name, 2, es)
    forward_response = query(index_name, event_name, field_name, 3, es)
    origin_count_list = count_in_each_interval(origin_response)
    comment_count_list = count_in_each_interval(comment_response)
    forward_count_list = count_in_each_interval(forward_response)

    # get elements for X axis
    timestamp_list = construct_X_axis(origin_response)

    # get heat indices
    heat_index_list = calculate_heat_index(origin_count_list, comment_count_list,
                                           forward_count_list, coefficient_for_heat_index)

    # generate heat curve result, including heat index, origin, comment and forward
    result_list_for_frontend = generate_heat_result_list(heat_index_list, origin_count_list,
                                                    comment_count_list, forward_count_list)

    return timestamp_list, result_list_for_frontend


def construct_X_axis(origin_response):
    '''
    comment and forward must happen after origin
    So the length of time interval depends on the first and the last origin post
    '''
    x_axis = []
    buckets = origin_response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        x_axis.append(buckets[i]["key"])

    return x_axis


def calculate_heat_index(origin_list, comment_list, forward_list, coefficient):
    # 此函数保留 总热度重新计算

    heat_index = []
    # temp = []

    for i in range(len(origin_list)):
       index = int(round(coefficient[0] * origin_list[i] + coefficient[1] * comment_list[i] \
                    + coefficient[2] * forward_list[i]))
       heat_index.append(index)

    # normalization
    # denominator = max(temp)
    # for i in range(len(origin_list)):
    #     heat_index.append(int(round((temp[i] / denominator) * 100)))

    return heat_index


def generate_heat_result_list(heat_list, origin_list, comment_list, forward_list):
    result_list = []

    for i in range(len(heat_list)):
        d = {'heat': heat_list[i], 'origin': origin_list[i],
             'comment': comment_list[i], 'forward': forward_list[i]}
        result_list.append(d)

    return result_list


def emotion_curve(index_name, event_name, es):

    field_name = "sentiment"

    # get num of positive and negative blog in each interval
    positive_response = query(index_name, event_name, field_name, 1, es)
    negative_response = query(index_name, event_name, field_name, 3, es)
    positive_count_list = count_in_each_interval(positive_response)
    negative_count_list = count_in_each_interval(negative_response)

    # calculate positive percent & negative percent
    positive_percentage, negative_percentage = \
        calculate_percentage(positive_count_list, negative_count_list)

    result_list_for_frontend = generate_emotion_result_list(positive_percentage,
                                                            negative_percentage)

    return negative_percentage, result_list_for_frontend


def calculate_percentage(positive_list, negative_list):

    total = []
    negative_percentage = []
    positive_percentage = []

    for i in range(len(positive_list)):
        total.append(positive_list[i] + negative_list[i])

    for i in range(len(positive_list)):
        if negative_list[i] == 0 and positive_list[i] == 0:
            positive_percentage.append(0)
            negative_percentage.append(0)

        elif negative_list[i] == 0 and positive_list[i] != 0:
            positive_percentage.append(1)
            negative_percentage.append(0)

        elif negative_list[i] != 0 and positive_list[i] == 0:
            positive_percentage.append(0)
            negative_percentage.append(1)

        else:
            negative_percentage.append(round((float(negative_list[i]) / total[i]), 2))
            positive_percentage.append(round((1 - negative_percentage[i]), 2))

    return positive_percentage, negative_percentage


def generate_emotion_result_list(positive_percentage, negative_percentage):
    result_list = []

    for i in range(len(positive_percentage)):
        negative_percentage[i] = int(negative_percentage[i] * 100)

    for i in range(len(positive_percentage)):
        positive_percentage[i] = int(positive_percentage[i] * 100)   # float to int

    for i in range(len(positive_percentage)):
        d = {'positive': positive_percentage[i], 'negative': negative_percentage[i]}
        result_list.append(d)

    return result_list


def query(index_name, event_name, field_name, value, es):

    query_body = {
        "size": 0,
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        field_name: value
                    }
                }
            }
        },
        "aggregations": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,  # global var
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = event_name,
        body = query_body)
    return response


def count_in_each_interval(response):
    '''
    get doc count
    '''
    counts = []
    buckets = response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        counts.append(buckets[i]["doc_count"])

    return counts


def baidu_news_heat_curve(event_name, es):

    result_list = []

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggregations": {
            "daily_news": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name_for_baidu_news,
        doc_type = event_name,
        body = query_body)
    buckets = response["aggregations"]["daily_news"]["buckets"]

    for i in range(len(buckets)):
        if buckets[i]['key'] < start_timestamp:
            continue
        elif buckets[i]['key'] > end_timestamp:
            break
        else:
            d = dict()
            d["timestamp"] = buckets[i]["key"]
            d["date"] = timestamp_to_date(buckets[i]["key"])
            d["num_of_baidu_news"] = buckets[i]["doc_count"]
            result_list.append(d)

    return result_list


def timestamp_to_date(unix_time):

    format = '%Y-%m-%d'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def wechat_article_heat_curve(event_name, es):

    result_list = []

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggregations": {
            "daily_article": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name_for_wechat_data,
        doc_type = event_name,
        body = query_body)

    buckets = response["aggregations"]["daily_article"]["buckets"]

    for i in range(len(buckets)):
        if buckets[i]['key'] < start_timestamp:
            continue
        elif buckets[i]['key'] > end_timestamp:
            break
        else:
            d = dict()
            d["timestamp"] = buckets[i]["key"]
            d["date"] = timestamp_to_date(buckets[i]["key"])
            d["num_of_wechat_article"] = buckets[i]["doc_count"]
            result_list.append(d)

    return result_list


def calculate_total_heat(blog_heat, baidu_news, wechat_article, coefficient):

    total_heat = []
    for i in range(len(blog_heat)):
        heat_value = blog_heat[i]["origin"] * coefficient[0] + blog_heat[i]["comment"] * coefficient[1] + \
        blog_heat[i]["forward"] * coefficient[2] + baidu_news[i]["num_of_baidu_news"] * coefficient[3] + \
        wechat_article[i]["num_of_wechat_article"] * coefficient[4]
        total_heat.append(int(round(heat_value)))

    return total_heat


def heat_and_emotion_interface(index_name, event_name, es):

    initialization(index_name, event_name, es)

    # get blog heat, news heat and article heat
    timestamp_list, blog_heat_result = blog_heat_curve(index_name, event_name, es)
    daily_baidu_news_count = baidu_news_heat_curve(event_name, es)
    daily_wechat_article_count = wechat_article_heat_curve(event_name, es)

    # get total heat
    total_heat = calculate_total_heat(blog_heat_result, daily_baidu_news_count,
                                      daily_wechat_article_count, coefficient_for_total_heat)

    # get negative emotion percentage
    negative_percentage, emotion_result = emotion_curve(index_name, event_name, es)

    return timestamp_list, blog_heat_result, emotion_result, negative_percentage, \
           daily_baidu_news_count, daily_wechat_article_count, total_heat


if __name__ == '__main__':

    index_name = "flow_text_data"
    event_name = "gangdu"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    heat_and_emotion_interface(index_name, event_name, es)
    et = time.time()

    print "heat and emotion running time: ", et - st