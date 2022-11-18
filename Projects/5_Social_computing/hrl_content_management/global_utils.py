# -*- coding: utf-8 -*-
'''
use to save table info in database
'''
import redis
from elasticsearch import Elasticsearch

from global_config import ES_SENSOR_HOST ,ES_FLOW_TEXT_HOST, ES_DATA_HOST, ES_FOR_UID_HOST, ES_INCIDENT_HOST, ES_MONITOR_HOST,ES_FOR_UID_MEDIA_HOST
from global_config import REDIS_HOST, REDIS_PORT


es_sensor = Elasticsearch(ES_SENSOR_HOST, timeout=600)
es_monitor = Elasticsearch(ES_MONITOR_HOST, timeout=600)
es_incident = Elasticsearch(ES_INCIDENT_HOST, timeout=600)
es_flow_text = Elasticsearch(ES_FLOW_TEXT_HOST, timeout=600)
es_data_text = Elasticsearch(ES_DATA_HOST, timeout=600)
es_for_uid = Elasticsearch(ES_FOR_UID_HOST, timeout=600)
es_for_uid_media = Elasticsearch(ES_FOR_UID_MEDIA_HOST, timeout=600)


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port)

# social sensing redis
R_SOCIAL_SENSING = _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=3)


# social sensing
index_manage_sensing = "manage_sensing_task"
type_manage_sensing = "task"

# event_sensing_text
index_content_sensing = "event_sensing_text"
type_content_sensing = "text"

# event_sensing_task
id_sensing_task = "event_sensing_task"

# flow_text
type_flow_text_index = "text"

# monitor_task
index_monitor_task = "monitor_task"
type_monitor_task = "task"

# monitor_task
index_incident_task = "group_incidents"
type_incident_task = "text"

# crawler: uid -> screen_name,type
index_name_for_uid = "uid_mapping_table"

# data for analysis
index_weixin_name = "weixin_data_text"
index_weibo_name = "weibo_data_text"
index_baidunews_name = "baidunews_data_text"


# data for media
index_name_for_media = "uid_media_table"
type_name_for_media = "text"



# HRL part
time_slice = 86400

# 这三个已经没用了
coefficient_for_heat_index = [0.2, 0.4, 0.4]
coefficient_for_total_heat = [0.1, 0.2, 0.2, 0.25, 0.25] # 原创 评论 转发 百度新闻 微信
coefficient_for_risk_index = [0.2, 0.2, 0.25, 0.15, 0.1, 0.1] # 总热度 情绪 敏感 uid数量 新闻源数量 公众号数量

coefficient_for_heat_risk = [0.35, 0.2, 0.45, 0.1, 0.1, 0.1]  # 媒体 帖子 地区
coefficient_for_trend_risk = [0.2, 0.35, 0.25, 0.2]  # 天数 媒体 帖子 地区
coefficient_for_emotion_risk = [0.3, 40, 50, 0.5, 1]  # 12负面占比 3冲突 45敏感帖子
coefficient_for_sensitive_risk = [5, 6, 10]  # 媒体 大v 敏感用户
coefficient_for_event_index = [0.3, 0.2, 0.4, 0.5, 0.4, 0.2, 0.3,  0.3, 40, 50, 0.5, 1,  5, 6, 10, 1, 1, 1]

# for risk_assessment_module_storage
# index_name_for_curve = "risk_evolution_curve_result"
# index_name_for_content = "risk_evolution_content_result"
index_name_for_baidu_news = "baidunews_data_text"
index_name_for_wechat_data = "weixin_data_text"


# for risk_point_module_propagation
index_name_for_uid_media_mapping = "uid_media_table"
event_name_for_uid_media_mapping = "text"

# micro
threshold_for_sensitive_value = 60
index_name_for_media_uid = "uid_media_table"

# macro
length_of_duration = 30

# threshold
threshold_for_num_of_media = 30
threshold_for_num_of_post = 3000
threshold_for_num_of_area = 300
threshold_for_media_propagation = 20
threshold_for_heat_propagation = 2000
threshold_for_area_propagation = 100
threshold_for_negative_emotion = 0.02
threshold_for_num_of_sensitive_blog = 10


# risk point storage
index_name_for_risk_point_curve = "risk_point_curve_result_test"
index_name_for_risk_point_content = "risk_point_details_test"
index_name_for_curve = "risk_evolution_curve_result"
index_name_for_content = "risk_evolution_content_result"

# event trend
index_name_for_uid_mapping_table = "uid_mapping_table"
event_name_for_uid_mapping_table = "text"

# subevent analysis
index_name_for_subevent_index = "combinatory_analysis"

# stage division
index_name_for_flow_text_data = 'flow_text_data'

index_name_for_subevent_storage = 'subevent_word_frequency'

index_name_for_prediction_data = 'prediction_data'  # 不再使用

num_of_prediction = 1

threshold_for_event_index_high = 2000
threshold_for_event_index_medium = 1200
threshold_for_event_index_low = 500

stage_division_evolution_to_climax = 1000

MySQL_host_ip = '219.224.134.226'
MySQL_host_port = 8088
MySQL_user = 'root'
MySQL_psd = '123456'
db_name = 'content_management'

# 内容管理
index_name_for_big_v = 'big_v_list'
index_name_for_key_media = 'key_media_list'
index_name_for_sensitive_user = 'sensitive_user_list'
index_name_for_sensitive_word = 'sensitive_words'
index_name_for_threshold = 'threshold'
index_name_for_weight = 'weight'
index_name_for_sensor = 'sensor_account'

index_name_for_facebook_data = 'facebook_data_text'
index_name_for_ftp_data = 'ftp_data_text'
index_name_for_twitter_data = 'twitter_data_text'

threshold_for_num_of_foreign_post = 300
# end of HRL part