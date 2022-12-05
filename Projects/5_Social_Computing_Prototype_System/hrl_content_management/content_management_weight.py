# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch

from global_utils import index_name_for_weight


def query_for_id(index_name, weight_name, es):

    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "weight": weight_name
                    }
                }
            }
        }
    }
    response = es.search(
        index = index_name,
        doc_type = 'text',
        body = query_body)
    if response['hits']['hits'] == []:
        return 0  # 未知错误
    else:
        id = response['hits']['hits'][0]['_id']
        return id


def modification_for_weight(index_name, weight_name, update_value, es):

    id = query_for_id(index_name, weight_name, es)
    if id == 0:
        return -1  # 未知错误，修改失败
    else:
        body = {"doc": {"value": update_value}}
        es.update(index=index_name, doc_type='text', id=id, body=body)
        time.sleep(1)
        print "update data successfully"
        return 1  # 添加成功


def query_all_for_weight(index_name, es):

    query_body = {
        "size": 100,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index = index_name,
        doc_type = 'text',
        body = query_body)
    content = response['hits']['hits']
    num = response['hits']['total']

    weight_list = []
    for item in content:
        d = dict()
        d['threshold_name'] = item['_source']['weight']
        d['value'] = item['_source']['value']
        weight_list.append(d)
    return weight_list, num


if __name__ == '__main__':
    es = Elasticsearch(['219.224.134.226:9207'])

    weight_name1 = 'weight_for_heat_index'
    query_result = query_for_id(index_name_for_weight, weight_name1, es)
    print query_result

    weight_name2 = 'weight_for_heat_index'
    update_value = '0.2 0.4 0.4'
    modification_result = modification_for_weight(index_name_for_weight, weight_name2, update_value, es)
    print modification_result

    query_all_result = query_all_for_weight(index_name_for_weight, es)
    print query_all_result