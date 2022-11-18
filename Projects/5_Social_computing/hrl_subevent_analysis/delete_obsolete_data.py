# -*- coding: utf-8 -*-

import time

from elasticsearch import Elasticsearch


def check_existence(index_name, doc_type, es):

    query_body = {
        "query":{
            "match_all": {}
        }
    }
    response = es.search(
        index = index_name,
        doc_type = doc_type,
        body = query_body)
    content = response['hits']['hits']
    length = response['hits']['total']

    if content == []:
        return False, length
    else:
        return True, length


def delete_interface(index_name, doc_type, es):

    exist, length = check_existence(index_name, doc_type, es)
    if exist:
        bulk_action = []
        for i in range(length):
            bulk_action.extend([{"delete": {"_id": i}}])  # 批量删除
            if i != 0 and i % 100 == 0:
                es.bulk(bulk_action, index = index_name, doc_type = doc_type)
                bulk_action = []
        if bulk_action:
            es.bulk(bulk_action, index = index_name, doc_type = doc_type)
        print 'delete old data successfully'
    else:
        print 'First time inserting data'
        return None


if __name__ == '__main__':

    index_name = "subevent_word_frequency"
    doc_type = "taidu_zhanzhong"
    es = Elasticsearch(['219.224.134.226:9207'])

    # initialization(index_name, event_name, es)
    st = time.time()
    delete_interface(index_name, doc_type, es)
    et = time.time()

    print "running time: ", et - st