# -*- coding: utf-8 -*-

import time
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# self defined module
from risk_assessment_module_details import details_interface
from risk_assessment_module_evolution_and_participant import curve_interface
from delete_obsolete_data import delete_interface

# global vars
from global_utils import index_name_for_curve
from global_utils import index_name_for_content
from global_utils import es_data_text


def pack_curve_result(event_name, timestamp, heat_curve_result,
                      emotion_result, key_user_curve_result, risk_result):
    table_for_curve = []

    for i in range(len(timestamp)):
        d = dict()
        d["type"] = event_name
        d["timestamp"] = timestamp[i]

        d["total_heat"] = heat_curve_result[i]["total_heat"]
        d["blog_heat"] = heat_curve_result[i]["blog_heat"]
        d["blog_origin"] = heat_curve_result[i]["blog_origin"]
        d["blog_comment"] = heat_curve_result[i]["blog_comment"]
        d["blog_forward"] = heat_curve_result[i]["blog_forward"]
        d["num_of_baidu_news"] = heat_curve_result[i]["num_of_baidu_news"]
        d["num_of_wechat_article"] = heat_curve_result[i]["num_of_wechat_article"]

        d["negative_percent"] = emotion_result[i]["negative"]
        d["positive_percent"] = emotion_result[i]["positive"]

        d["risk_index"] = risk_result[i]["risk_index"]
        d["cumulative_risk_index"] = risk_result[i]["cumulative_risk_index"]
        d["heat_risk"] = risk_result[i]["heat_risk"]
        d["emotion_risk"] = risk_result[i]["emotion_risk"]
        d["sensitive_risk"] = risk_result[i]["sensitive_risk"]

        d["num_of_blog_participant"] = key_user_curve_result[i]["num_of_blog_participant"]
        d["num_of_news_source"] = key_user_curve_result[i]["num_of_news_source"]
        d["num_of_article_source"] = key_user_curve_result[i]["num_of_article_source"]
        table_for_curve.append(d)

    return table_for_curve


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


def index_data_for_curve(index_name, event_name, curve_result, es):

    bulk_action = []
    index_count = 0

    for i in range(len(curve_result)):

        d = dict()
        d["timestamp"] = curve_result[i]["timestamp"]

        d["total_heat"] = curve_result[i]["total_heat"]
        d["blog_heat"] = curve_result[i]["blog_heat"]

        d["blog_origin"] = curve_result[i]["blog_origin"]
        d["blog_comment"] = curve_result[i]["blog_comment"]
        d["blog_forward"] = curve_result[i]["blog_forward"]
        d["num_of_blog"] = curve_result[i]["blog_origin"] + curve_result[i]["blog_comment"] + \
                           curve_result[i]["blog_forward"]

        d["num_of_baidu_news"] = curve_result[i]["num_of_baidu_news"]
        d["num_of_wechat_article"] = curve_result[i]["num_of_wechat_article"]

        d["negative_percent"] = curve_result[i]["negative_percent"]
        d["positive_percent"] = curve_result[i]["positive_percent"]

        d["risk_index"] = curve_result[i]["risk_index"]
        d["cumulative_risk_index"] = curve_result[i]["cumulative_risk_index"]
        d["heat_risk"] = curve_result[i]["heat_risk"]
        d["emotion_risk"] = curve_result[i]["emotion_risk"]
        d["sensitive_risk"] = curve_result[i]["sensitive_risk"]

        d["num_of_blog_participant"] = curve_result[i]["num_of_blog_participant"]
        d["num_of_news_source"] = curve_result[i]["num_of_news_source"]
        d["num_of_article_source"] = curve_result[i]["num_of_article_source"]
        # d["key_users"] = json.dumps(curve_result[i]["key_users"])   # 可以删除

        d["sequence_number"] = index_count
        d["date_of_calculation"] = timestamp_to_date(time.time())

        bulk_action.extend([{"index": {"_id": index_count}}, d])
        index_count += 1

        if index_count != 0 and index_count % 100 == 0:
            es.bulk(bulk_action, index = index_name, doc_type = event_name)
            bulk_action = []

    if bulk_action:
        es.bulk(bulk_action, index = index_name, doc_type = event_name)

    print "index curve data successfully"


def index_data_for_content(index_name, event_name, content_result, es):

    bulk_action = []
    index_count = 0

    for i in range(len(content_result)):
        for j in range(len(content_result[i])):
            d = dict()
            d["timestamp"] = content_result[i][j]["timestamp"]
            d["mid"] = str(content_result[i][j]["mid"])
            d["num_of_comment"] = content_result[i][j]["comment"]
            d["num_of_forward"] = content_result[i][j]["forward"]
            d["uid"] = content_result[i][j]["uid"]
            d["total_count"] = content_result[i][j]["total_count"]
            d["datetime"] = content_result[i][j]["datetime"]
            d["text"] = content_result[i][j]["text"]
            d["user_name"] = content_result[i][j]["user_name"]
            d["date_of_calculation"] = timestamp_to_date(time.time())

            bulk_action.extend([{"index": {"_id": index_count}}, d])
            index_count += 1

            if index_count != 0 and index_count % 100 == 0:
                es.bulk(bulk_action, index = index_name, doc_type = event_name)
                bulk_action = []

    if bulk_action:
        es.bulk(bulk_action, index = index_name, doc_type = event_name)

    print "index content data successfully"


def timestamp_to_date(unix_time):
    '''
    convert unix timestamp to datetime
    '''
    format = '%Y-%m-%d'

    value = time.localtime(unix_time)
    date = time.strftime(format, value)

    return date


def store_result_to_ES(index_name, event_name, es):
    '''
    interface of the module
    invoke this function to store processing result into ES

    :param event_name: name of event

    :return: none
    '''
    # get 4-curve results
    timestamp_list, heat_curve_result, emotion_result, \
    participation_curve_result, risk_curve_result = curve_interface(index_name, event_name, es)

    # pack all curve result, stored in a dic
    curve_result = pack_curve_result(event_name, timestamp_list, heat_curve_result, emotion_result,
                                     participation_curve_result, risk_curve_result)
    # risk details
    risk_details_result = details_interface(index_name, event_name, es)

    # 创建索引
    create_index_for_curve(index_name_for_curve, es)
    create_index_for_content(index_name_for_content, es)

    # 删除旧数据
    delete_interface(index_name_for_curve, event_name, es)
    delete_interface(index_name_for_content, event_name, es)

    # 索引数据
    index_data_for_curve(index_name_for_curve, event_name, curve_result, es)
    index_data_for_content(index_name_for_content, event_name, risk_details_result, es)

    return None


if __name__ == '__main__':

    index_name = "weibo_data_text"
    event_name = "林丹"
    # 如果索引里没有该事件的数据，则报错(数组越界)

    st = time.time()
    store_result_to_ES(index_name, event_name, es_data_text)
    et = time.time()

    print "storage time: ", et - st
