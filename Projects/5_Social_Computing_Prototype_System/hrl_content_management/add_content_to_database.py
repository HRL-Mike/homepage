# -*- coding: utf-8 -*-

import time
import json

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch

from global_utils import index_name_for_weight
from global_utils import index_name_for_threshold
from global_utils import index_name_for_sensitive_word
from global_utils import index_name_for_big_v
from global_utils import index_name_for_key_media
from global_utils import index_name_for_sensitive_user
from global_utils import index_name_for_sensor


def create_index_for_user_list(index_name, es):
    '''
    创建用户列表索引
    '''
    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        },
        "mappings": {
            "user_list": {
                "properties": {
                    "name": {
                        "index": "not_analyzed",
                        "type": "string"
                    },
                    "uid": {
                        "index": "not_analyzed",
                        "type": "string"
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=create_body, ignore=400)


def create_index_for_sensitive_word(index_name, es):
    '''
    创建敏感词索引
    '''
    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        },
        "mappings": {
            "sensitive_word": {
                "properties": {
                    "sensitive_word": {
                        "index": "not_analyzed",
                        # 设置为not_analyzed表示不分词 可以进行子串匹配
                        # 默认为analyzed 逐字分词
                        "type": "string"
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=create_body, ignore=400)


def create_index_for_threshold(index_name, es):
    '''
    创建阈值索引
    '''
    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        },
        "mappings": {
            "threshold": {
                "properties": {
                    "threshold": {
                        "index": "not_analyzed",
                        "type": "string"
                    },
                    "value": {
                        "index": "not_analyzed",
                        "type": "string"
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=create_body, ignore=400)


def create_index_for_weight(index_name, es):
    '''
    创建权重索引
    '''
    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        },
        "mappings": {
            "weight": {
                "properties": {
                    "weight": {
                        "index": "not_analyzed",
                        "type": "string"
                    },
                    "value": {
                        "index": "not_analyzed",
                        "type": "string"
                    }
                }
            }
        }
    }
    es.indices.create(index=index_name, body=create_body, ignore=400)


def get_sensitive_word_from_txt(file_path, index_name, es):
    '''
    读取并添加敏感词
    '''
    sensitive_word_list = []
    with open(file_path, 'r') as f:
        for line in f:
            word = line.strip()
            sensitive_word_list.append(word.decode('utf-8'))

    if not sensitive_word_list:
        print "输入的敏感词列表为空，添加失败"
        return 0   # 列表为空，添加失败

    # 检查输入列表是否包含重复项
    for word in sensitive_word_list:
        if sensitive_word_list.count(word) > 1:
            print "输入列表包含重复敏感词，添加失败"
            return 0

    # 插入操作
    counter = 0
    bulk_action = []
    for i in range(len(sensitive_word_list)):
        d = dict()
        d["sensitive_word"] = sensitive_word_list[i]
        d["date_of_insertion"] = timestamp_to_date(time.time())
        bulk_action.extend([{"index": {"_id": int(time.time() + i)}}, d])
        counter += 1

        if counter != 0 and counter % 100 == 0:
            es.bulk(bulk_action, index=index_name, doc_type='text')
            time.sleep(0.5)
            bulk_action = []

    if bulk_action:
        es.bulk(bulk_action, index=index_name, doc_type='text')
        time.sleep(1)  # 确保添加操作完成
        print "成功添加敏感词"
        return 1  # 添加成功
    else:
        print "未知错误，添加敏感词失败"
        return 0  # 添加失败


def get_user_info_from_txt(file_path, index_name, es):
    '''
    读取并添加用户数据
    '''
    user_list = []
    uid_list = []
    with open(file_path, 'r') as f:
        for line in f:
            user_info = line.strip().split(' ')
            user_list.append(user_info[0])
            '''
            如果遇到上面这一行报解码错误
            解决方法：打开txt文件，左上角文件-另存为
            弹出的对话框最下侧编码选择UTF-8，覆盖原文件
            所有txt文件都执行此操作
            '''
            uid_list.append(int(user_info[1]))

    if not user_list:
        print "输入的用户名列表为空，添加失败"
        return 0

    if not uid_list:
        print "输入的uid列表为空，添加失败"
        return 0

    # 检查输入列表是否包含重复项
    for user in user_list:
        if user_list.count(user) > 1:
            print "输入列表中包含重复用户名，添加失败"
            return 0

    for uid in uid_list:
        if uid_list.count(uid) > 1:
            print "输入列表中包含重复的uid，添加失败"
            return 0

    if len(user_list) == len(uid_list):
        # 插入操作
        counter = 0
        bulk_action = []
        for i in range(len(user_list)):
            d = dict()
            d["name"] = user_list[i]
            d["uid"] = uid_list[i]
            d["channel"] = 0
            bulk_action.extend([{"index": {"_id": uid_list[i]}}, d])
            counter += 1

            if counter != 0 and counter % 100 == 0:
                es.bulk(bulk_action, index = index_name, doc_type = 'text')
                time.sleep(0.5)
                bulk_action = []

        if bulk_action:
            es.bulk(bulk_action, index = index_name, doc_type = 'text')
            time.sleep(1)  # 确保添加操作完成
            print "成功添加用户数据"
            return 1  # 添加成功
        else:
            print "未知错误，添加用户数据失败"
            return 0  # 添加失败
    else:
        print "用户列表和uid列表长度不匹配"
        return 0



def add_threshold(index_name, es):
    '''
    添加阈值
    '''
    key = ['threshold_for_sensitive_value', 'length_of_duration', 'threshold_for_num_of_media',
           'threshold_for_num_of_post', 'threshold_for_num_of_area', 'threshold_for_media_propagation', 'threshold_for_heat_propagation',
           'threshold_for_area_propagation', 'threshold_for_negative_emotion', 'threshold_for_num_of_sensitive_blog', 'threshold_for_event_index_high',
           'threshold_for_event_index_medium', 'threshold_for_event_index_low', 'stage_division_evolution_to_climax', 'threshold_for_num_of_foreign_post']
    value = [60, 30, 30, 3000, 300, 20, 2000, 100, 0.02, 10, 2000, 1200, 500, 1000, 300]

    bulk_action = []
    for i in range(len(key)):
        d = dict()
        d['threshold'] = key[i]
        d['value'] = json.dumps(value[i])
        d["last_modification_date"] = timestamp_to_date(time.time())
        bulk_action.extend([{"index": {"_id": int(time.time() + i)}}, d])

    if bulk_action:
        es.bulk(bulk_action, index = index_name, doc_type = 'text')
        print "成功添加阈值数据"


def add_weight(index_name, es):
    '''
    添加权重
    '''
    w_name = ['weight_for_heat_index', 'weight_for_total_heat', 'weight_for_risk_index',
              'weight_for_heat_risk', 'weight_for_trend_risk', 'weight_for_emotion_risk',
              'weight_for_sensitive_risk', 'weight_for_event_index']

    weight = ['0.2 0.4 0.4', '0.1 0.2 0.2 0.25 0.25', '0.2 0.2 0.25 0.15 0.1 0.1',
              '0.35 0.2 0.45 0.1 0.1 0.1', '0.2 0.35 0.25 0.2', '0.3 40 50 0.5 1', '5 6 10',
              '0.3 0.2 0.4 0.5 0.4 0.2 0.3 0.3 40 50 0.5 1 5 6 10 1 1 1']

    bulk_action = []
    for i in range(len(weight)):
        d = dict()
        d['value'] = json.dumps(weight[i])
        d['weight'] = w_name[i]
        d["last_modification_date"] = timestamp_to_date(time.time())
        bulk_action.extend([{"index": {"_id": int(time.time() + i)}}, d])

    if bulk_action:
        es.bulk(bulk_action, index=index_name, doc_type='text')
        print "成功添加权重数据"


def timestamp_to_date(unix_time):
    '''
    convert unix timestamp to datetime
    '''
    format = '%Y-%m-%d'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


if __name__ == '__main__':

    es = Elasticsearch(['219.224.134.225:9225'])

    # 创建索引
    create_index_for_weight(index_name_for_weight, es)
    create_index_for_threshold(index_name_for_threshold, es)
    create_index_for_sensitive_word(index_name_for_sensitive_word, es)
    create_index_for_user_list(index_name_for_big_v, es)
    create_index_for_user_list(index_name_for_key_media, es)
    create_index_for_user_list(index_name_for_sensitive_user, es)
    create_index_for_user_list(index_name_for_sensor, es)

    # 插入操作
    add_weight(index_name_for_weight, es)
    add_threshold(index_name_for_threshold, es)

    # 记得修改路径
    sensitive_word_file_path = 'E:/Projects/LocalRepository/GroupEvent/group/group_event/content_management/sensitive_words.txt'
    get_sensitive_word_from_txt(sensitive_word_file_path, index_name_for_sensitive_word, es)

    big_v_file_path = 'E:/Projects/LocalRepository/GroupEvent/group/group_event/content_management/big_v.txt'
    key_media_file_path = 'E:/Projects/LocalRepository/GroupEvent/group/group_event/content_management/key_media.txt'
    sensitive_user_file_path = 'E:/Projects/LocalRepository/GroupEvent/group/group_event/content_management/sensitive_user.txt'
    sensor_file_path = 'E:/Projects/LocalRepository/GroupEvent/group/group_event/content_management/sensor.txt'
    get_user_info_from_txt(big_v_file_path, index_name_for_big_v, es)
    get_user_info_from_txt(key_media_file_path, index_name_for_key_media, es)
    get_user_info_from_txt(sensitive_user_file_path, index_name_for_sensitive_user, es)
    get_user_info_from_txt(sensor_file_path, index_name_for_sensor, es)

