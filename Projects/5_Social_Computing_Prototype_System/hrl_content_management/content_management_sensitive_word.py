# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from global_utils import index_name_for_sensitive_word


def check_existence(index_name, field, word_list, es):
    '''
    检查列表中各敏感词的存在性，若存在，返回重复项
    '''

    duplicate = []
    for word in word_list:
        query_body = {
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            field: word
                        }
                    }
                }
            }
        }
        response = es.search(
            index=index_name,
            doc_type='text',
            body=query_body)
        if response['hits']['hits'] == []:
            continue
        else:
            duplicate.append(response['hits']['hits'][0]['_source']['sensitive_word'])
    return duplicate


def add_sensitive_word(index_name, word_in_str, es):

    # 检查输入是否为空
    if not word_in_str:
        return 0  # 添加失败

    # 检查输入列表是否包含重复项
    word_list = word_in_str.split(',')   # 无法保证编码完全不报错
    for word in word_list:
        if word_list.count(word) > 1:
            return -1  # 输入列表中包含重复项

    field = "sensitive_word"
    # 检查数据库是否包含重复词
    duplicate = check_existence(index_name, field,  word_list, es)
    # print duplicate
    if not duplicate:  # 不包含
        bulk_action = []
        for i in range(len(word_list)):
            d = dict()
            d[field] =  word_list[i]
            d["date_of_insertion"] = timestamp_to_date(time.time())
            bulk_action.extend([{"index": {"_id": int(time.time()+i)}}, d])

        if bulk_action:
            es.bulk(bulk_action, index=index_name, doc_type='text')
            time.sleep(1)  # 确保添加操作完成
            print "index data successfully"
            return 1  # 添加成功
        else:
            print "Error: index data failed"
            return 0  # 添加失败
    else:
        return duplicate  # 添加列表中包含已存在的词，未执行添加操作


def timestamp_to_date(unix_time):
    '''
    convert unix timestamp to datetime
    '''
    format = '%Y-%m-%d'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def query_sensitive_word(index_name, query_word, es):

    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "sensitive_word": query_word
                    }
                }
            }
        }
    }
    response = es.search(
        index=index_name,
        doc_type='text',
        body=query_body)
    if response['hits']['hits'] == []:
        return -1  # 查无此词
    else:
        d = dict()
        d['sensitive_word'] = query_word
        d['id'] = response['hits']['hits'][0]['_id']
        return d  # 查询成功 返回一个字典 包含词和其id


def delete_sensitive_word(index_name, delete_word, es):  # 要删除的词一定是存在的

    # 先查id再删除
    query_result = query_sensitive_word(index_name, delete_word, es)
    if query_result == -1:
        return 0
    else:
        es.delete(index=index_name, doc_type='text', id=query_result['id'])
        time.sleep(1) # 确保删除操作完成
        later_query_result = query_sensitive_word(index_name, delete_word, es)
        # print later_query_result
    if later_query_result == -1:
        return 1  # 删除成功
    else:
        return 0  # 删除失败


def fuzzy_query_for_sensitive_word(index_name, query_word, es):

    query_body = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "sensitive_word": '*' + query_word + '*'
                        }
                    }
                ]
            }
        }
    }
    response = es.search(
        index=index_name,
        doc_type='text',
        body=query_body)
    content = response['hits']['hits']
    count = 0
    word_list = []
    for i in range(len(content)):
        word_list.append(content[i]['_source']['sensitive_word'])
        count+=1
    return word_list,count


def query_all_for_sensitive_word(index_name, es_client):

    query_body = {
        "query": {
            "match_all": {}
        }
    }

    es_result = helpers.scan(
        client = es_client,
        query = query_body,
        scroll = "2m",
        index = index_name,
        doc_type = "text",
        timeout = "5m")

    count = 0
    sensitive_word_list = []
    for item in es_result:
        sensitive_word_list.append(item['_source']['sensitive_word'])
        count += 1
    return sensitive_word_list, count


if __name__ == '__main__':

    es = Elasticsearch(['219.224.134.226:9207'])

    # create_index_for_sensitive_word(index_name_for_sensitive_word, es)
    #
    # file_with_path = './sensitive_words.txt'
    # word_str = get_sensitive_word_from_txt(file_with_path)
    # add_result = add_sensitive_word(index_name_for_sensitive_word, word_str, es)
    # print add_result
    #
    # query_word = '北京事件'.decode('utf-8')
    # query_result = query_sensitive_word(index_name_for_sensitive_word, query_word, es)
    # print query_result
    # #
    # delete_word = '北京事件'.decode('utf-8')
    # delete_result = delete_sensitive_word(index_name_for_sensitive_word, delete_word, es)
    # print delete_result
    #
    # query_word2 = '北京'
    # fuzzy_query_result = fuzzy_query_for_sensitive_word(index_name_for_sensitive_word, query_word2, es)
    # print fuzzy_query_result
    #
    result = query_all_for_sensitive_word(index_name_for_sensitive_word, es)