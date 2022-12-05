# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from global_utils import index_name_for_big_v
from global_utils import index_name_for_key_media
from global_utils import index_name_for_sensitive_user
from global_utils import index_name_for_sensor


'''本模块不提供接口，views直接调用模块中的各函数'''

def add_new_item(index_name, user_in_str, es):
    # 输入示例："北京青年报,1749990115"
    # 一次只能添加一条

    # 分离用户和uid
    input_list = user_in_str.split(',')   # 使用utf-8解码会报错
    input_name = input_list[0]
    input_uid = int(input_list[1])

    bulk_action = []
    d = dict()
    d['name'] = input_name
    d['uid'] = input_uid
    d['channel'] = 1
    bulk_action.extend([{"index": {"_id": input_uid}}, d])

    if bulk_action:
        es.bulk(bulk_action, index=index_name, doc_type='text')
        time.sleep(1)  # 确保添加操作完成

    # 检查插入是否成功
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "uid": input_uid
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
    if content != []:
        name = content[0]["_source"]['name']
        if name == input_name:
            return 1  # 添加成功
        else:
            return -1  # 添加失败
    else:
        return -1  # 添加失败


def query_user(index_name, query_uid, es):
    # 查询某个uid是否存在，若存在，返回相关信息
    # 内部函数，非调用

    query_body = {
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "uid": query_uid
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
        return -1  # 查无此人
    else:
        d = dict()
        d['name'] = response['hits']['hits'][0]['_source']['name']
        d['uid'] = response['hits']['hits'][0]['_source']['uid']
        d['id'] = response['hits']['hits'][0]['_id']
        return d  # 查询成功 返回一个字典 包含名字和uid


def delete_multiple_users(index_name, uid_str, es):
    # 删除多个用户
    # 输入示例："1883818283,2065240833,1848555562"
    uid_list = uid_str.split(',')

    for uid in uid_list:
        result = delete_user(index_name, uid, es)
        if result == False:
            return 0
    time.sleep(1)  # 确保删除操作完成

    for uid in uid_list:
        later_query_result = query_user(index_name, uid, es)
        if later_query_result != -1:
            return 0
    return 1


def delete_user(index_name, delete_uid, es):
    # 内部函数，非调用

    # 先查id再删除
    query_result = query_user(index_name, delete_uid, es)
    if query_result == -1:
        return False  # 查无此uid，删除失败
    else:
        es.delete(index=index_name, doc_type='text', id=query_result['id'])
        return True


def fuzzy_query_for_user_list(index_name, fuzzy_word, es):
    # 模糊查询功能
    # 拿输入项分别去匹配name和uid，返回所有匹配的项
    query_body = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "name": '*' + fuzzy_word + '*'
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
    num1 = response['hits']['total']

    query_body2 = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "uid": '*' + fuzzy_word + '*'
                        }
                    }
                ]
            }
        }
    }
    response = es.search(
        index=index_name,
        doc_type='text',
        body=query_body2)
    content2 = response['hits']['hits']
    num2 = response['hits']['total']

    user_list = []
    for i in range(len(content)):
        d = dict()
        d['name'] = content[i]['_source']['name']
        d['uid'] = content[i]['_source']['uid']
        user_list.append(d)
    for i in range(len(content2)):
        d = dict()
        d['name'] = content2[i]['_source']['name']
        d['uid'] = content2[i]['_source']['uid']
        user_list.append(d)
    total_num = num1 + num2
    return user_list, total_num


def query_all_for_user_list(index_name, es_client):
    # 查询所有用户数据
    query_body = {
        "query": {
            "match_all": {}
        }
    }

    # 这种写法避免了使用size来指定返回的最大数目
    es_result = helpers.scan(
        client=es_client,
        query=query_body,
        scroll="2m",
        index=index_name,
        doc_type="text",
        timeout="5m")

    count = 0
    user_list = []
    for item in es_result:
        d = dict()
        d['name'] = item['_source']['name']
        d['uid'] = item['_source']['uid']
        d['channel'] = item['_source']['channel']
        user_list.append(d)
        count += 1
    return user_list, count


def check_consistency(index_name, str, es):
    '''
    检查数据一致性，确保插入操作的合法性
    '''
    input_list = str.split(',')
    if len(input_list[1]) != 10 or not input_list[1].isdigit():
        return -2   # 长度非法或uid非数字

    input_name = input_list[0]
    input_uid = int(input_list[1])
    query_body = {
        "query": {
            "bool": {
                "must": [{
                    "term": {
                        "name": input_name
                    }
                }]
            }
        }
    }
    response = es.search(
        index=index_name,
        doc_type='text',
        body=query_body)
    content = response['hits']['hits']
    if content != []:
        return 0   # 昵称存在，拒绝添加
    else:
        query_body = {
            "query": {
                "bool": {
                    "must": [{
                        "term": {
                            "uid": input_uid
                        }
                    }]
                }
            }
        }
        response = es.search(
            index=index_name,
            doc_type='text',
            body=query_body)
        content = response['hits']['hits']
        if content == []:
            return 1   # 可以添加
        else:
            name = content[0]['_source']['name']
            uid = content[0]['_source']['uid']
            if uid == input_uid and name != input_name:
                return -1   # uid匹配但昵称不同


def update_user(index_name, str, es):
    input_list = str.split(',')  # 使用utf-8解码会报错
    input_name = input_list[0]
    input_uid = int(input_list[1])

    body = {"doc": {"name": input_name, "channel": 1}}
    es.update(index=index_name, doc_type='text', id=input_uid, body=body)
    time.sleep(1)
    query_body = {
        "query": {
            "bool": {
                "must": [
                {
                    "term": {
                        "uid": input_uid
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
    if content != []:
        name = content[0]["_source"]['name']
        if name == input_name:
            return 1 # 更新成功
        else:
            return -1 # 更新失败
    else:
        return -1 # 更新失败


if __name__ == '__main__':

    es = Elasticsearch(['219.224.134.226:9207'])

    # big_v_str = "我是化妆女王,1682192071"
    # sensitive_user_str = "笑话,2814680487"
    # key_media_str = "人民网,2286908003"
    # check_result = check_consistency(index_name_for_big_v, big_v_str, es)
    # print check_result

    # big_v_str2 = "我是化妆女王,1682192071"
    # update_user(index_name_for_big_v, big_v_str2, es)

    #print key_media_str.decode('gbk')  # 北京青年报,1749990115,中央人民广播电台,1867571077
    # add_result1 = add_new_item(index_name_for_big_v, big_v_str, es)
    # add_result2 = add_new_item(index_name_for_key_media, key_media_str, es)
    # add_result3 = add_new_item(index_name_for_sensitive_user, sensitive_user_str, es)
    # # print add_result2
    #
    # # 查询某一条记录，主要为了获取记录的id，自用
    # query_content = 1378010100
    # query_content2 = '王子文Olivia'
    # query_result1 = query_user(index_name_for_big_v, query_content, es)
    # query_result2 = query_user(index_name_for_key_media, query_content2, es)
    # query_result3 = query_user(index_name_for_sensitive_user, query_content, es)
    # print query_result1  # 字典
    # print query_result2  # -1
    #
    # # 删除一条记录，输入为uid
    # delete_uid1 = '1378010100'.decode('utf-8')
    # delete_uid2 = '1749990115'.decode('utf-8')
    # delete_uid3 = '1899936227'.decode('utf-8')
    # delete_result1 = delete_user(index_name_for_big_v, delete_uid1, es)
    # delete_result2 = delete_user(index_name_for_key_media, delete_uid2, es)
    # delete_result3 = delete_user(index_name_for_sensitive_user, delete_uid3, es)
    # print delete_result1
    # print delete_result2
    # print delete_result3
    #
    # # 模糊匹配
    # fuzzy_word = '搞笑'
    # fuzzy_query_result, total_num = fuzzy_query_for_user_list(index_name_for_sensitive_user, fuzzy_word, es)
    # print len(fuzzy_query_result)
    # print total_num
    #
    # # 返回所有记录
    # result = query_all_for_user_list(index_name_for_sensitive_user, es)