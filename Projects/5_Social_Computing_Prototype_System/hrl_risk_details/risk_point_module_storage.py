# -*- coding: utf-8 -*-

import time
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from elasticsearch import Elasticsearch

from risk_point_module_calculation_and_judgement import risk_point_interface
from risk_point_module_details import details_interface
from calculate_initiator import initiator_interface
from calculate_pusher import pusher_interface
from stage_division import risk_stage_division
from prediction import prediction_interface
from delete_obsolete_data import delete_interface
from global_utils import index_name_for_risk_point_curve
from global_utils import index_name_for_risk_point_content
from global_utils import es_data_text
from global_utils import num_of_prediction
from global_utils import time_slice
from global_utils import threshold_for_event_index_high
from global_utils import threshold_for_event_index_medium
from global_utils import threshold_for_event_index_low


def create_index_for_curve(index_name, es):
    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        }
    }
    es.indices.create(index = index_name, body = create_body, ignore = 400)


def create_index_for_content(index_name, es):
    create_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 0
        }
    }
    es.indices.create(index = index_name, body = create_body, ignore = 400)


def index_data_for_curve(index_name, event_name, es, timestamp, final_list, heat_risk, trend_risk,
                         emotion_risk, sensitive_risk, event_index, initiator_uid, pusher_uid, stage_list,
                         evolution_duration, climax_duration):
    bulk_action = []
    index_count = 0
    sub_index = len(heat_risk) - 1

    # 补齐预测天数的时间戳
    temp_timestamp = timestamp[-1]
    for i in range(num_of_prediction):
        temp_timestamp = temp_timestamp + time_slice
        timestamp.append(temp_timestamp)

    for i in range(len(event_index)):
        if i <= sub_index:
            d = dict()
            d["timestamp"] = timestamp[i]
            d["date"] = timestamp_to_date(timestamp[i])

            d["heat_risk"] = heat_risk[i]
            d["trend_risk"] = trend_risk[i]
            d["emotion_risk"] = emotion_risk[i]
            d["sensitive_risk"] = sensitive_risk[i]
            d["event_index"] = event_index[i]

            if event_index[i] < threshold_for_event_index_low:
                d['risk_level'] = 0  # 无风险
            if event_index[i] >= threshold_for_event_index_low and event_index[i] < threshold_for_event_index_medium:
                d['risk_level'] = 1  # 低风险
            elif event_index[i] >= threshold_for_event_index_medium and event_index[i] < threshold_for_event_index_high:
                d['risk_level'] = 2  # 中等风险
            elif event_index[i] >= threshold_for_event_index_high:
                d['risk_level'] = 3  # 高风险

            if final_list[i] != []:
                d["risk_point"] = json.dumps(final_list[i])
            else:
                d["risk_point"] = None

            d["sequence_number"] = index_count
            d["date_of_calculation"] = timestamp_to_date(time.time())

            d['initiator_uid'] = initiator_uid
            d['pusher_uid'] = pusher_uid
            d["evolution_duration"] = evolution_duration
            d["climax_duration"] = climax_duration

            d['stage'] = stage_list[i]

            bulk_action.extend([{"index": {"_id": index_count}}, d])
            index_count += 1

            if index_count != 0 and index_count % 100 == 0:
                es.bulk(bulk_action, index = index_name, doc_type = event_name)
                bulk_action = []
        else:
            d = dict()
            d["timestamp"] = timestamp[i]
            d["date"] = None

            d["heat_risk"] = None
            d["trend_risk"] = None
            d["emotion_risk"] = None
            d["sensitive_risk"] = None
            d["event_index"] = event_index[i]
            d["risk_point"] = None

            if event_index[i] < threshold_for_event_index_low:
                d['risk_level'] = 0
            if event_index[i] >= threshold_for_event_index_low and event_index[i] < threshold_for_event_index_medium:
                d['risk_level'] = 1
            elif event_index[i] >= threshold_for_event_index_medium and event_index[i] < threshold_for_event_index_high:
                d['risk_level'] = 2
            elif event_index[i] >= threshold_for_event_index_high:
                d['risk_level'] = 3

            d["sequence_number"] = index_count
            d["date_of_calculation"] = timestamp_to_date(time.time())

            d['initiator_uid'] = initiator_uid
            d['pusher_uid'] = pusher_uid
            d["evolution_duration"] = evolution_duration
            d["climax_duration"] = climax_duration

            d['stage'] = stage_list[i]

            bulk_action.extend([{"index": {"_id": index_count}}, d])
            index_count += 1

            if index_count != 0 and index_count % 100 == 0:
                es.bulk(bulk_action, index=index_name, doc_type=event_name)
                bulk_action = []

    if bulk_action:
        es.bulk(bulk_action, index = index_name, doc_type = event_name)

    print "index risk point curve data successfully"


def timestamp_to_date(unix_time):
    '''
    convert unix timestamp to datetime
    '''
    format = '%Y-%m-%d'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def  index_data_for_content(index_name, event_name, es, media_post, bigv_post,
                            sensitive_user_post, sensitive_blog, negative_blog):

    post_list = pack_content(media_post, bigv_post, sensitive_user_post, sensitive_blog, negative_blog)
    index_data(post_list, index_name, event_name, es)

    return None


def pack_content(media_post, bigv_post, sensitive_user_post, sensitive_blog, negative_blog):

    post_list = []
    for media in media_post:
        post_list.append(media)

    for bigv in bigv_post:
        post_list.append(bigv)

    for sensitive_user in sensitive_user_post:
        post_list.append(sensitive_user)

    for blog in sensitive_blog:
        post_list.append(blog)

    for blog in negative_blog:
        post_list.append(blog)

    return post_list


def index_data(post, index_name, event_name, es):
    bulk_action = []
    index_count = 0

    for i in range(len(post)):
        d = dict()
        d["uid"] = post[i]["uid"]
        d["user_name"] = post[i]["user_name"]
        d["text"] = post[i]["text"]
        d["mid"] = post[i]["mid"]
        d["keywords_string"] = post[i]["keywords_string"]
        d["real_timestamp"] = post[i]["real_timestamp"]
        d["sensitive"] = post[i]["sensitive"]
        d["timestamp"] = post[i]["timestamp"]
        d["type"] = post[i]["type"]
        d["date_of_calculation"] = timestamp_to_date(time.time())
        d["date"] = timestamp_to_date_with_second(post[i]["real_timestamp"])
        d['num_of_comment_and_forward'] = post[i]['num_of_comment_and_forward']

        bulk_action.extend([{"index": {"_id": index_count}}, d])
        index_count += 1

        if index_count != 0 and index_count % 100 == 0:
            es.bulk(bulk_action, index = index_name, doc_type = event_name)
            bulk_action = []

    if bulk_action:
        es.bulk(bulk_action, index = index_name, doc_type = event_name)

    print "index risk point content successfully"
    return None


def timestamp_to_date_with_second(unix_time):
    '''
    convert unix timestamp to datetime
    '''
    format = '%Y-%m-%d %H:%M:%S'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def calculate_evolution_and_climax_duration(stage_list):
    '''
    计算发酵和高潮阶段的持续时间
    '''

    index_a = 0
    index_b = 0
    index_c = 0
    evolution_duration = 0
    climax_duration = 0
    for i in range(len(stage_list)):
        if stage_list[i] == 0:
            continue
        if stage_list[i] == 1:
            index_a = i
        if stage_list[i] == 2:
            index_b = i
            evolution_duration = index_b - index_a
        if stage_list[i] == 3:
            index_c = i
            climax_duration = climax_duration + (index_c - index_b)

    return evolution_duration, climax_duration


def get_risk_level(event_index):

    risk_level = []
    for index in event_index:
        if index < threshold_for_event_index_low:
            risk_level.append(0)
        elif  index >= threshold_for_event_index_low and index < threshold_for_event_index_medium:
            risk_level.append(1)
        elif index >= threshold_for_event_index_medium and index < threshold_for_event_index_high:
            risk_level.append(2)
        elif index > threshold_for_event_index_high:
            risk_level.append(3)
    return risk_level


def storage_interface(index_name, event_name, es):

    # 调用接口获取曲线数据
    timestamp, final_list, key_media_list, bigv_list, sensitive_user_list, \
    heat_risk, trend_risk, emotion_risk, sensitive_risk, event_index = risk_point_interface(index_name, event_name, es)
    print 'anchor a'

    # 调用接口获取代表内容
    media_post, bigv_post, sensitive_user_post, \
    sensitive_blog, negative_blog = details_interface(index_name, event_name, es)
    # storage调用details,details调用macro和micro
    print 'anchor b'

    # 计算发起者&推动者
    initiator_uid = initiator_interface(index_name, event_name, es)
    pusher_uid = pusher_interface(index_name, event_name, es)
    print 'anchor c'

    # 先计算预测点，再进行阶段划分
    event_index = prediction_interface(event_index)
    stage_list = risk_stage_division(event_index)
    evolution_duration, climax_duration = calculate_evolution_and_climax_duration(stage_list)
    print 'anchor d'

    # 创建索引
    create_index_for_curve(index_name_for_risk_point_curve, es)
    create_index_for_content(index_name_for_risk_point_content, es)

    # 删除旧数据
    delete_interface(index_name_for_risk_point_curve, event_name, es)
    delete_interface(index_name_for_risk_point_content, event_name, es)

    # 索引数据
    index_data_for_curve(index_name_for_risk_point_curve, event_name, es, timestamp, final_list, heat_risk,
                         trend_risk, emotion_risk, sensitive_risk, event_index, initiator_uid, pusher_uid,
                         stage_list, evolution_duration, climax_duration)
    index_data_for_content(index_name_for_risk_point_content, event_name, es,
                           media_post, bigv_post, sensitive_user_post, sensitive_blog, negative_blog)

    return None


if __name__ == '__main__':

    # "weibo_data_text"
    index_name = "weibo_data_text"
    event_name = "林丹"

    # initialization(index_name, event_name, es)
    st = time.time()
    storage_interface(index_name, event_name, es_data_text)
    et = time.time()

    print "running time: ", et - st