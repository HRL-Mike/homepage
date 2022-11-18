# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from elasticsearch import Elasticsearch

from risk_point_module_micro import micro_interface
from risk_point_module_macro import macro_interface

from global_utils import time_slice
from global_utils import threshold_for_num_of_media
from global_utils import threshold_for_num_of_post
from global_utils import threshold_for_num_of_area
from global_utils import length_of_duration
from global_utils import threshold_for_media_propagation
from global_utils import threshold_for_heat_propagation
from global_utils import threshold_for_area_propagation
from global_utils import threshold_for_negative_emotion
from global_utils import threshold_for_num_of_sensitive_blog
from global_utils import coefficient_for_heat_risk
from global_utils import coefficient_for_trend_risk
from global_utils import coefficient_for_emotion_risk
from global_utils import coefficient_for_sensitive_risk
from global_utils import coefficient_for_event_index
from global_utils import threshold_for_num_of_foreign_post


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


def get_timestamp(index_name, event_name, es):

    timestamp = []

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
        timestamp.append(buckets[i]["key"])

    return timestamp


def judge_num_of_media(num_of_media, final_list):

    for i in range(len(num_of_media)):
        if num_of_media[i] >= threshold_for_num_of_media:
            temp = ["type_A", num_of_media[i]]
            final_list[i].append(temp)
    return final_list


def judge_num_of_post(num_of_post, final_list):

    for i in range(len(num_of_post)):
        if num_of_post[i] >= threshold_for_num_of_post:
            temp = ["type_B", num_of_post[i]]
            final_list[i].append(temp)
    return final_list


def judge_num_of_area(num_of_area, final_list):

    for i in range(len(num_of_area)):
        if num_of_area[i] >= threshold_for_num_of_area:
            temp = ["type_C", num_of_area[i]]
            final_list[i].append(temp)
    return final_list


def judge_duration(target_timestamp, final_list):

    if target_timestamp != 0:
        temp = ["type_D", length_of_duration]
        final_list[length_of_duration-1].append(temp)
    return final_list


def judge_media_propagation(media_propagation, final_list):

    for i in range(len(media_propagation)):
        if media_propagation[i] >= threshold_for_media_propagation:
            temp = ["type_E", media_propagation[i]]
            final_list[i].append(temp)
    return final_list


def judge_heat_propagation(heat_propagation, final_list):

    for i in range(len(heat_propagation)):
        if heat_propagation[i] >= threshold_for_heat_propagation:
            temp = ["type_F", heat_propagation[i]]
            final_list[i].append(temp)
    return final_list


def judge_area_propagation(area_propagation, final_list):

    for i in range(len(area_propagation)):
        if area_propagation[i] >= threshold_for_area_propagation:
            temp = ["type_G", area_propagation[i]]
            final_list[i].append(temp)
    return final_list


def judge_negative_emotion_ratio(negative_emotion, final_list):

    for i in range(len(negative_emotion)):
        if negative_emotion[i] >= threshold_for_negative_emotion:
            temp = ["type_H", negative_emotion[i]]
            final_list[i].append(temp)
    return final_list


def judge_emotion_conflict(conflict_result, final_list):

    for i in range(len(conflict_result)):
        if conflict_result[i] != 0:
            temp = ["type_I"]
            final_list[i].append(temp)
    return final_list


def judge_num_of_sensitive_blog(sensitive_blog, final_list):

    for i in range(len(sensitive_blog)):
        if sensitive_blog[i] >= threshold_for_num_of_sensitive_blog:
            temp = ["type_J", sensitive_blog[i]]
            final_list[i].append(temp)
    return final_list


def judge_key_media_attendance(media_attendance_list, media_attendance_record, final_list):

    timestamp = start_timestamp
    uid_list = []
    duration = len(media_attendance_list) # 天数

    for a in range(duration):
        uid_list.append(list())

    # 格式转换 现在记录每天有哪些uid出现
    for i in range(duration):
        for j in range(len(media_attendance_record)):  # uid_list长度
            for t in media_attendance_record[j]["timestamp"]:
                if t == timestamp:
                    uid_list[i].append(media_attendance_record[j]["uid"])
        timestamp = timestamp + time_slice
    # print media_attendance_list
    # for k in range(len(uid_list)):
    #     print uid_list[k]
    #print uid_list

    for b in range(duration):
        if media_attendance_list[b] != 0:
            temp = ["type_K", media_attendance_list[b]]  # 关键媒体数量
            final_list[b].append(temp)

    return final_list, uid_list


def judge_bigv_attendance(bigv_attendance_list, bigv_attendance_record, final_list):

    duration = len(bigv_attendance_list)
    uid_list = []
    timestamp = start_timestamp

    for a in range(duration):
        uid_list.append(list())

    # 格式转换 现在记录每天有哪些uid出现
    for i in range(duration):
        for j in range(len(bigv_attendance_record)):  # uid_list长度
            for t in bigv_attendance_record[j]["timestamp"]:
                if t == timestamp:
                    uid_list[i].append(bigv_attendance_record[j]["uid"])
        timestamp = timestamp + time_slice

    # print bigv_attendance_list
    # print uid_list

    for b in range(duration):
        if bigv_attendance_list[b] != 0:
            temp = ["type_L", bigv_attendance_list[b]]
            final_list[b].append(temp)

    return final_list, uid_list


def judge_sensitive_user_attendance(sensitive_user_list, sensitive_user_record, final_list):

    duration = len(sensitive_user_list)
    uid_list = []
    timestamp = start_timestamp

    for a in range(duration):
        uid_list.append(list())

    # 格式转换 现在记录每天有哪些uid出现
    for i in range(duration):
        for j in range(len(sensitive_user_record)):  # uid_list长度
            for t in sensitive_user_record[j]["timestamp"]:
                if t == timestamp:
                    uid_list[i].append(sensitive_user_record[j]["uid"])
        timestamp = timestamp + time_slice

    # print sensitive_user_list
    # print uid_list

    for b in range(duration):
        if sensitive_user_list[b] != 0:
            temp = ["type_M", sensitive_user_list[b]]
            final_list[b].append(temp)
    return final_list, uid_list


def judge_num_of_foreign_post(num_of_fb, num_of_tt, num_of_ftp, final_list):

    for i in range(len(num_of_fb)):
        total = num_of_fb[i] + num_of_tt[i] + num_of_ftp[i]
        if total >= threshold_for_num_of_foreign_post:  # 境外帖子数量大于300则标记为风险
            temp = ["type_N", total]
            final_list[i].append(temp)
    return final_list


def calculate_heat_risk(media, post, area, coefficient, num_of_fb, num_of_tt, num_of_ftp):

    heat_risk = []
    for i in range(len(media)):
        value = media[i] * coefficient[0] + post[i] * coefficient[1] + area[i] * coefficient[2] + \
                num_of_fb[i] * coefficient[3] + num_of_tt[i] * coefficient[4] + num_of_ftp[i] * coefficient[5]
        heat_risk.append(round(value, 2))

    return heat_risk


def calculate_trend_risk(media, heat, area, coefficient):

    days = []
    trend_risk = []

    for i in range(len(media)):
        days.append(i+1)

    for j in range(len(media)):
        if media[j] <= 0:  # 改变了列表中的值
            media[j] = 0
        if heat[j] <= 0:
            heat[j] = 0
        if area[j] <= 0:
            area[j] = 0
        value = days[j] * coefficient[0] + media[j] * coefficient[1] + \
                heat[j] * coefficient[2] + area[j] * coefficient[3]
        trend_risk.append(round(value, 2))

    return trend_risk


def calculate_emotion_risk(negative_ratio, conflict, sensitive_blog, coefficient):

    emotion_risk = []

    for i in range(len(negative_ratio)):
        if negative_ratio[i] >= threshold_for_negative_emotion:
            coefficient_ratio = coefficient[1]
        else:
            coefficient_ratio = coefficient[0]

        if sensitive_blog[i] >= threshold_for_num_of_sensitive_blog:
            coefficient_blog = coefficient[4]
        else:
            coefficient_blog = coefficient[3]

        value = negative_ratio[i] * coefficient_ratio + conflict[i] * coefficient[2] + \
                sensitive_blog[i] * coefficient_blog
        emotion_risk.append(round(value, 2))

    return emotion_risk


def calculate_sensitive_risk(media_attendance, bigv_attendance, sensitive_user_attendance, coefficient):

    sensitive_risk = []

    for i in range(len(media_attendance)):
        value = media_attendance[i] * coefficient[0] + bigv_attendance[i] * coefficient[1] + \
                sensitive_user_attendance[i] * coefficient[2]
        sensitive_risk.append(value)

    return sensitive_risk


def calculate_event_index(num_of_media, num_of_post, num_of_area, media_propagation_rate, heat_propagation_rate,
                           area_propagation_rate, negative_emotion_ratio, conflict_result, num_of_sensitive_blog,
                           media_attendance_list, bigv_attendance_list, sensitive_user_attendance_list,
                           coefficient, num_of_fb, num_of_tt, num_of_ftp):
    days = []
    event_index = []

    for i in range(len(num_of_media)):
        days.append(i + 1)

    for i in range(len(num_of_media)):
        if negative_emotion_ratio[i] >= threshold_for_negative_emotion:
            coefficient_ratio = coefficient[8]
        else:
            coefficient_ratio = coefficient[7]

        if num_of_sensitive_blog[i] >= threshold_for_num_of_sensitive_blog:
            coefficient_blog = coefficient[11]
        else:
            coefficient_blog = coefficient[10]

        value = num_of_media[i] * coefficient[0] + num_of_post[i] * coefficient[1] + num_of_area[i] * coefficient[2] + \
                days[i] * coefficient[3] + media_propagation_rate[i] * coefficient[4] + \
                heat_propagation_rate[i] * coefficient[5] + area_propagation_rate[i] * coefficient[6] + \
                negative_emotion_ratio[i] * coefficient_ratio + conflict_result[i] * coefficient[9] + \
                num_of_sensitive_blog[i] * coefficient_blog + media_attendance_list[i] * coefficient[12] + \
                bigv_attendance_list[i] * coefficient[13] + sensitive_user_attendance_list[i] * coefficient[14] + \
                num_of_fb[i] * coefficient[15] + num_of_tt[i] * coefficient[16] + num_of_ftp[i] * coefficient[17]
        event_index.append(round(value, 2))
    return event_index


def risk_point_interface(index_name, event_name, es):

    initialization(index_name, event_name, es)

    # 获取时间轴
    timestamp_list = get_timestamp(index_name, event_name, es)

    # 调用接口获取各类宏观风险点的计算数据
    num_of_media, num_of_post, num_of_area, target_timestamp, \
    media_propagation_rate, heat_propagation_rate, area_propagation_rate, \
    num_of_fb, num_of_tt, num_of_ftp = macro_interface(index_name, event_name, es)

    # 调用接口获取各类微观风险点的计算数据
    negative_emotion_ratio, conflict_result, num_of_sensitive_blog, media_attendance_list, media_attendance_record, \
    bigv_attendance_list, bigv_attendance_record, sensitive_user_attendance_list, sensitive_user_attendance_record, \
    num_of_blog_with_sensitive_word = micro_interface(index_name, event_name, es)

    # 创建存储判断结果的列表
    final_list = []
    for i in range(len(num_of_media)):
        final_list.append(list())

    # 判断各时间段内是否存在某类风险点
    final_list = judge_num_of_media(num_of_media, final_list)
    final_list = judge_num_of_post(num_of_post, final_list)
    final_list = judge_num_of_area(num_of_area, final_list)
    final_list = judge_num_of_foreign_post(num_of_fb, num_of_tt, num_of_ftp, final_list)

    final_list = judge_duration(target_timestamp, final_list)
    final_list = judge_media_propagation(media_propagation_rate, final_list)
    final_list = judge_heat_propagation(heat_propagation_rate, final_list)
    final_list = judge_area_propagation(area_propagation_rate, final_list)

    final_list = judge_negative_emotion_ratio(negative_emotion_ratio, final_list)
    final_list = judge_emotion_conflict(conflict_result, final_list)
    final_list = judge_num_of_sensitive_blog(num_of_sensitive_blog, final_list)

    final_list, key_media_list = judge_key_media_attendance(media_attendance_list, media_attendance_record, final_list)
    final_list, bigv_list = judge_bigv_attendance(bigv_attendance_list, bigv_attendance_record, final_list)
    final_list, sensitive_user_list = judge_sensitive_user_attendance(sensitive_user_attendance_list,
                                                                      sensitive_user_attendance_record, final_list)
    # 计算4种风险值，线性计算
    heat_risk = calculate_heat_risk(num_of_media, num_of_post, num_of_area,
                                    coefficient_for_heat_risk, num_of_fb, num_of_tt, num_of_ftp)
    trend_risk = calculate_trend_risk(media_propagation_rate, heat_propagation_rate,
                                      area_propagation_rate, coefficient_for_trend_risk)
    emotion_risk = calculate_emotion_risk(negative_emotion_ratio, conflict_result,
                                          num_of_sensitive_blog, coefficient_for_emotion_risk)
    sensitive_risk = calculate_sensitive_risk(media_attendance_list, bigv_attendance_list,
                             sensitive_user_attendance_list, coefficient_for_sensitive_risk)
    # 计算事件指数
    event_index = calculate_event_index(num_of_media, num_of_post, num_of_area, media_propagation_rate,
                                         heat_propagation_rate, area_propagation_rate, negative_emotion_ratio,
                                         conflict_result, num_of_sensitive_blog, media_attendance_list,
                                         bigv_attendance_list, sensitive_user_attendance_list,
                                         coefficient_for_event_index, num_of_fb, num_of_tt, num_of_ftp)

    return timestamp_list, final_list, key_media_list, bigv_list, sensitive_user_list, \
           heat_risk, trend_risk, emotion_risk, sensitive_risk, event_index


if __name__ == '__main__':

    index_name = "flow_text_data"
    event_name = "gangdu"
    es = Elasticsearch(['219.224.134.226:9207'])

    initialization(index_name, event_name, es)
    st = time.time()
    risk_point_interface(index_name, event_name, es)
    et = time.time()

    print "running time: ", et - st
