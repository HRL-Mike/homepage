# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time

from global_utils import time_slice
from global_utils import threshold_for_sensitive_value

from global_utils import es_data_text
from global_utils import index_name_for_big_v
from global_utils import index_name_for_key_media
from global_utils import index_name_for_sensitive_user
from global_utils import index_name_for_sensitive_word


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
                    "interval": time_slice,
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


# 负面情绪强度
def get_negative_emotion_intensity(index_name, event_name, es):

    # 微博帖子总数
    num_of_blog = get_num_of_blog(index_name, event_name, es)
    # 微博负面情绪帖子数量
    num_of_negative_blog = get_num_of_emotional_blog(index_name, event_name, es, 3)
    # 计算负面情绪强度
    negative_emotion_ratio = calculate_negative_emotion_intensity(num_of_blog, num_of_negative_blog)

    return negative_emotion_ratio


def get_num_of_blog(index_name, event_name, es):

    num_of_blog = []

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggs": {
            "num_of_blog": {
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
        index = index_name,
        doc_type = event_name,
        body = query_body)

    buckets = response["aggregations"]["num_of_blog"]["buckets"]

    for i in range(len(buckets)):
        num_of_blog.append(buckets[i]["doc_count"])

    return num_of_blog


def get_num_of_emotional_blog(index_name, event_name, es, value):

    num_of_emotional_blog = []

    query_body = {
        "size": 0,
        "query": {
            "filtered": {
                "filter": {
                    "term": {
                        "sentiment": value
                    }
                }
            }
        },
        "aggregations": {
            "time_slice": {
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
        index = index_name,
        doc_type = event_name,
        body = query_body)

    buckets = response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        num_of_emotional_blog.append(buckets[i]["doc_count"])

    return num_of_emotional_blog


def calculate_negative_emotion_intensity(blog, negative_blog):

    ratio_list = []

    for i in range(len(blog)):
        if blog[i] == 0:
            ratio_list.append(0.0)
        else:
            ratio = negative_blog[i] / float(blog[i])
            ratio_list.append(round(ratio, 3))

    return ratio_list


# 情感冲突
def get_emotion_conflict_judgement(index_name, event_name, es):

    num_of_blog = get_num_of_blog(index_name, event_name, es)
    num_of_negative_blog = get_num_of_emotional_blog(index_name, event_name, es, 3)
    num_of_positive_blog = get_num_of_emotional_blog(index_name, event_name, es, 1)

    judge_result = judge_if_conflict_happen(num_of_blog, num_of_negative_blog, num_of_positive_blog)

    return judge_result


def judge_if_conflict_happen(blog, negative_blog, positive_blog):

    result_list = []

    for i in range(len(blog)):
        if blog[i] >= 600 and abs(negative_blog[i] - positive_blog[i]) < 50:
            result_list.append(1)
        else:
            result_list.append(0)

    return result_list


# 敏感帖子数量
def get_num_of_sensitive_blog(index_name, event_name, es):
    '''
    计算敏感值大于阈值的帖子数量
    '''

    num_of_sensitive_blog = []

    query_body = {   # range的用法，还是query和aggs搭配使用
        "size": 0,
        "query": {
            "range": {
                "sensitive": {
                    "gte": threshold_for_sensitive_value
                }
            }
        },
        "aggregations": {
            "time_slice": {
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
        index = index_name,
        doc_type = event_name,
        body = query_body)

    buckets = response["aggregations"]["time_slice"]["buckets"]

    for i in range(len(buckets)):
        num_of_sensitive_blog.append(buckets[i]["doc_count"])

    return num_of_sensitive_blog


# 关键媒体是否参与
def get_key_media_attendance(index_name, event_name, es):

    # 获取关键媒体的uid列表
    media_uid_list = get_media_uid(index_name_for_key_media, es)
    # 各时间段内有哪些媒体出现，数量是多少
    media_attendance_list, media_attendance_record = check_attendance(media_uid_list, index_name, event_name, es)

    return media_attendance_list, media_attendance_record


def get_media_uid(index_name, es):

    uid_list = []
    query_body = {
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index = index_name,
        doc_type = 'text',
        body = query_body)
    content = response['hits']['hits']

    for item in content:
        uid_list.append(item['_source']['uid'])

    # db = pymysql.connect(host=MySQL_host_ip, port=MySQL_host_port, user=MySQL_user,
    #                      passwd=MySQL_psd, db=db_name, charset="utf8")
    # cursor = db.cursor()
    #
    # sql_select = '''select * from key_media_table'''
    # try:
    #     cursor.execute(sql_select)
    #     # 获取所有记录列表
    #     result = cursor.fetchall()
    #     for row in result:
    #         uid_list.append(row[1])
    # except:
    #     print "Error: get media uid failed"
    # db.close()

    return uid_list


def check_attendance(uid_list, index_name, event_name, es):

    attendance_list = []  # 0表示无uid出现，非零值i表示有i个uid出现
    attendance_record = []  # 记录了哪个uid在哪些时间段里出现了，为提取相关帖子做准备

    for i in range(len(uid_list)):
        query_body = {
            "size": 0,
            "query": {
                "filtered": {
                    "filter": {
                        "term": {
                            "uid": uid_list[i]
                        }
                    }
                }
            },
            "aggregations": {
                "time_slice": {
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
            index = index_name,
            doc_type = event_name,
            body = query_body)

        buckets = response["aggregations"]["time_slice"]["buckets"]

        temp = []
        timestamp_record = []
        for j in range(len(buckets)):
            if buckets[j]["doc_count"] == 0:
                temp.append(0)
            else:
                temp.append(buckets[j]["doc_count"])
                timestamp_record.append(buckets[j]["key"])

        attendance_list.append(temp)
        d = dict()
        d["uid"] = uid_list[i]
        d["timestamp"] = timestamp_record
        attendance_record.append(d)

    # print len(attendance_list)
    # print len(attendance_record)
    # for y in range(len(attendance_list)):
    #     print attendance_list[y]
    # for z in range(len(attendance_record)):
    #     print attendance_record[z]

    merged_attendance_list = [0] * len(attendance_list[0])  # uid_list的长度
    for k in range(len(attendance_list)):
        for z in range(len(attendance_list[k])):
            merged_attendance_list[z] = merged_attendance_list[z] + attendance_list[k][z]

    # print merged_attendance_list
    return merged_attendance_list, attendance_record


# 大V参与
def get_bigv_attendance(index_name, event_name, es):

    # 算法与get_key_media_attendance相似
    bigv_uid_list = get_bigv_uid(index_name_for_big_v, es)

    bigv_attendance_list, bigv_attendance_record = check_attendance(bigv_uid_list, index_name, event_name, es)

    return bigv_attendance_list, bigv_attendance_record


def get_bigv_uid(index_name, es):

    uid_list = []
    query_body = {
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index=index_name,
        doc_type='text',
        body=query_body)
    content = response['hits']['hits']

    for item in content:
        uid_list.append(item['_source']['uid'])

    # db = pymysql.connect(host=MySQL_host_ip, port=MySQL_host_port, user=MySQL_user,
    #                      passwd=MySQL_psd, db=db_name, charset="utf8")
    # cursor = db.cursor()
    #
    # sql_select = '''select * from big_v_table'''
    # try:
    #     cursor.execute(sql_select)
    #     # 获取所有记录列表
    #     result = cursor.fetchall()
    #     for row in result:
    #         uid_list.append(row[1])
    # except:
    #     print "Error: get big v uid failed"
    # db.close()

    return uid_list


# 敏感用户参与
def get_sensitive_user_attendance(index_name, event_name, es):

    # 算法与get_key_media_attendance相似
    sensitive_user_list = get_sensitive_user(index_name_for_sensitive_user, es)

    sensitive_user_attendance_list, sensitive_user_attendance_record = \
        check_attendance(sensitive_user_list, index_name, event_name, es)

    return sensitive_user_attendance_list, sensitive_user_attendance_record


def get_sensitive_user(index_name, es):

    uid_list = []
    query_body = {
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index=index_name,
        doc_type='text',
        body=query_body)
    content = response['hits']['hits']

    for item in content:
        uid_list.append(item['_source']['uid'])

    # db = pymysql.connect(host=MySQL_host_ip, port=MySQL_host_port, user=MySQL_user,
    #                      passwd=MySQL_psd, db=db_name, charset="utf8")
    # cursor = db.cursor()
    #
    # sql_select = '''select * from sensitive_user_table'''
    # try:
    #     cursor.execute(sql_select)
    #     # 获取所有记录列表
    #     result = cursor.fetchall()
    #     for row in result:
    #         uid_list.append(row[1])
    # except:
    #     print "Error: get sensitive user uid failed"
    # db.close()

    return uid_list


# 含有关键敏感词的帖子
def get_sensitive_words_attendance(index_name, event_name, es):

    sensitive_word_list = get_sensitive_words(index_name_for_sensitive_word, es)

    num_of_blog_with_sensitive_word = check_sensitive_word_attendance(sensitive_word_list, index_name, event_name, es)

    return num_of_blog_with_sensitive_word


def get_sensitive_words(index_name, es):
    print("index_name",index_name)
    sensitive_word_list = []
    query_body = {
        "size": 10000,
        "query": {
            "match_all": {}
        }
    }
    response = es.search(
        index = index_name,
        doc_type = 'text',
        body = query_body)
    content = response['hits']['hits']

    for item in content:
        sensitive_word_list.append(item['_source']['sensitive_word'])

    # db = pymysql.connect(host=MySQL_host_ip, port=MySQL_host_port, user=MySQL_user,
    #                      passwd=MySQL_psd, db=db_name, charset="utf8")
    # cursor = db.cursor()
    #
    # sql_select = '''select * from sensitive_words'''
    # try:
    #     cursor.execute(sql_select)
    #     # 获取所有记录列表
    #     result = cursor.fetchall()
    #     for row in result:
    #         sensitive_word_list.append(row[0])
    # except:
    #     print "Error: get sensitive words failed"
    # db.close()

    return sensitive_word_list


def check_sensitive_word_attendance(sensitive_word_list, index_name, event_name, es):

    query_list = []
    num_of_blog_with_sensitive_word = []  # 含关键敏感词的帖子的数量

    if sensitive_word_list:
        for sensitive_word in sensitive_word_list:
            query_list.append({'wildcard':{'text': '*' + sensitive_word + '*'}})

    query_body = {
        "size": 0,
        "query": {
            "bool": {
                "should": query_list,
                "minimum_should_match": 3
            }
        },
        "aggregations": {
            "sensitive_words": {
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
    response = es.search(index = index_name,doc_type = event_name,body = query_body)

    buckets = response["aggregations"]["sensitive_words"]["buckets"]

    for i in range(len(buckets)):
        num_of_blog_with_sensitive_word.append(buckets[i]["doc_count"])

    # print num_of_blog_with_sensitive_word
    return num_of_blog_with_sensitive_word


def micro_interface(index_name, event_name, es):

    initialization(index_name, event_name, es)

    # 负面情绪强度
    negative_emotion_ratio = get_negative_emotion_intensity(index_name, event_name, es)
    # 情感冲突
    judge_result = get_emotion_conflict_judgement(index_name, event_name, es)
    # 敏感帖子数量
    num_of_sensitive_blog = get_num_of_sensitive_blog(index_name, event_name, es)

    # 关键媒体参与情况(有多少，有哪些)
    media_attendance_list, media_attendance_record = get_key_media_attendance(index_name, event_name, es)
    # 大V参与情况(有多少，有哪些)
    bigv_attendance_list, bigv_attendance_record = get_bigv_attendance(index_name, event_name, es)
    # 敏感用户参与情况(有多少，有哪些)
    sensitive_user_attendance_list, sensitive_user_attendance_record = get_sensitive_user_attendance(index_name, event_name, es)

    # 含有关键敏感词的帖子的数量
    num_of_blog_with_sensitive_word = get_sensitive_words_attendance(index_name, event_name, es)

    return negative_emotion_ratio, judge_result, num_of_sensitive_blog, \
           media_attendance_list, media_attendance_record, bigv_attendance_list, bigv_attendance_record, \
           sensitive_user_attendance_list, sensitive_user_attendance_record, num_of_blog_with_sensitive_word


def micro_interface_for_details(index_name, event_name, es):
    '''
    专门为代表内容部分写的接口
    '''

    initialization(index_name, event_name, es)

    # 关键媒体参与情况(有多少uid & 各关键媒体的uid在哪个时间段里出现)
    media_attendance_list, media_attendance_record = get_key_media_attendance(index_name, event_name, es)
    # 大V参与情况(有多少uid & 各大V的uid在哪个时间段里出现)
    bigv_attendance_list, bigv_attendance_record = get_bigv_attendance(index_name, event_name, es)
    # 敏感用户参与情况(有多少uid & 各敏感用户的uid在哪个时间段里出现)
    sensitive_user_attendance_list, sensitive_user_attendance_record = get_sensitive_user_attendance(index_name, event_name, es)

    # 负面情绪强度
    negative_emotion_ratio = get_negative_emotion_intensity(index_name, event_name, es)
    # 敏感帖子数量
    num_of_sensitive_blog = get_num_of_sensitive_blog(index_name, event_name, es)

    return media_attendance_list, media_attendance_record, bigv_attendance_list, bigv_attendance_record, \
           sensitive_user_attendance_list, sensitive_user_attendance_record, num_of_sensitive_blog, \
           negative_emotion_ratio


if __name__ == '__main__':

    index_name = "weibo_data_text"
    event_name = "滴滴"
    # es = Elasticsearch([{"host":"219.224.134.226","port":9207,"timeout":600}])

    st = time.time()
    micro_interface(index_name, event_name, es_data_text)
    et = time.time()

    print "running time: ", et - st