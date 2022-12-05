# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time

from elasticsearch import Elasticsearch

from global_utils import index_name_for_flow_text_data


def get_events(index_name, combination_name, es):

    query_body = {
        "size": 10,
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "event_name": combination_name
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = "text",
        body = query_body)
    content = response["hits"]["hits"][0]
    events = content["_id"].split('_')

    return events


def get_key_word_list(index_name, event_name, es):
    '''
    :return: a list contains keywords for all blog about certain event
    '''

    key_word_list = []
    query_body = {
        "size": 1000000,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index = index_name,   # flow_text_data 修正为 weibo_data_text
        doc_type = event_name,
        body = query_body)
    content = response['hits']['hits']

    # get keywords from "keywords_string" for each blog, store keywords in a list
    for i in range(len(content)):
    	try:
        	str = content[i]['_source']['keywords_string']
        	list = str.split('&')
        	key_word_list = key_word_list + list
        except:
        	pass

    return key_word_list


def get_dual_occurrence_word(events, key_word_list):  # 求最大共现次数

    left_pointer = 0
    length = len(key_word_list)-1
    dual_occurrence_list = []
    dual_event_list = []
    # 求两个事件关键词的交集，并记录是哪两个事件
    for i in range(length):
        right_pointer = left_pointer + 1
        while right_pointer <= length:
            dual_occurrence_list.append(list(set(key_word_list[left_pointer]).intersection(set(key_word_list[right_pointer]))))
            dual_event_list.append([events[left_pointer], events[right_pointer]])
            right_pointer += 1
        left_pointer += 1

    return dual_occurrence_list, dual_event_list


def calculate_frequency(index_name, dual_occurrence_word_list, dual_event_list, es):

    word_frequency_list = []
    for i in range(len(dual_occurrence_word_list)):
        temp = dict()
        for j in range(len(dual_occurrence_word_list[i])):
            query_body = {
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "wildcard": {
                                    "keywords_string": '*' + dual_occurrence_word_list[i][j] + '*'
                                }
                            }
                        ]
                    }
                }
            }
            response = es.search(
                index = index_name,
                doc_type = dual_event_list[i][0],
                body = query_body)
            num1 = response['hits']['total']

            query_body_2 = {
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "wildcard": {
                                    "keywords_string": '*' + dual_occurrence_word_list[i][j] + '*'
                                }
                            }
                        ]
                    }
                }
            }
            response_2 = es.search(
                index = index_name,
                doc_type = dual_event_list[i][1],
                body = query_body_2)
            num2 = response_2['hits']['total']

            frequency = num1 + num2
            temp[dual_occurrence_word_list[i][j]] = frequency   # temp is a dic
        word_frequency_list.append(temp)

    return word_frequency_list


def get_sum(word_frequency_list):

    d = dict()
    for i in range(len(word_frequency_list)):
        for key in word_frequency_list[i].keys():
            if key not in d:
                d[key] = word_frequency_list[i].get(key)
            else:
                d[key] = d.get(key) + word_frequency_list[i].get(key)
    # a = {'a': 3, 'b': 2, 'c': 3, 'e': 2}
    # b = {'a': 2, 'b': 2, 'c': 4, 'e': 1}
    # return: {'a': 3, 'b': 2, 'c': 4, 'e': 2}
    return d


def sort_and_filter(word_frequency_result):

    sorted_result = sorted(word_frequency_result.items(), key = lambda x: x[1], reverse = True)
    filtered_result = sorted_result[0:300] # 返回前300

    return filtered_result


def subevent_word_interface(events, es):

    # 获取事件列表
    event_list = events.split('_')

    key_word_list = []
    for item in event_list:
        temp = get_key_word_list(index_name_for_flow_text_data, item, es)
        key_word_list.append(temp)


    # 求共现两次的关键词并记录对应事件
    dual_occurrence_word_list, dual_event_list = get_dual_occurrence_word(event_list, key_word_list)
    # 计算各事件包含共现关键词的微博数量，求和作为词频
    word_frequency_list = calculate_frequency(index_name_for_flow_text_data, dual_occurrence_word_list, dual_event_list, es)
    # cat在AB中出现了3次，在BC中出现了5次，则cat总共出现了8次
    word_frequency_result = get_sum(word_frequency_list)
    # 按词频排序，取前300作为高频共现词
    result = sort_and_filter(word_frequency_result)

    return result


if __name__ == '__main__':

    events = "台独_占中_港独"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    subevent_word_interface(events, es)
    et = time.time()

    print "running time: ", et - st