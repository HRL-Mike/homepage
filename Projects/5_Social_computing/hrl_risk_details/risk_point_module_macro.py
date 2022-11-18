# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from elasticsearch import Elasticsearch

from global_utils import time_slice
from global_utils import index_name_for_uid_media_mapping
from global_utils import event_name_for_uid_media_mapping
from global_utils import index_name_for_baidu_news
from global_utils import index_name_for_wechat_data
from global_utils import length_of_duration
from global_utils import index_name_for_facebook_data
from global_utils import index_name_for_ftp_data
from global_utils import index_name_for_twitter_data


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
                    "interval": time_slice,  # global var
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


# 媒体覆盖情况
def get_num_of_media(index_name, event_name, es):

    field_name_baidu = "news_source"
    field_name_wechat = "source"

    num_of_blog_media = get_num_of_blog_media(index_name, event_name, es)
    # 微博媒体数量较少，uid_media_table里的媒体数量有限

    baidu_source_count = source_count(index_name_for_baidu_news, event_name, es, field_name_baidu)
    wechat_source_count = source_count(index_name_for_wechat_data, event_name, es, field_name_wechat)

    num_of_media = calculate_num_of_media(num_of_blog_media, baidu_source_count, wechat_source_count)

    return num_of_media


def get_num_of_blog_media(index_name, event_name, es):
    '''
    统计各时间段内的媒体uid的数量
    返回一个列表，包含各时间段媒体uid的数量
    '''
    num_of_blog_media = []

    # 获取各时间段内的uid
    query_body = {
        "size": 0,
        "aggs": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                },
                "aggs": {
                    "uid": {
                        "terms": {
                            "field": "uid",
                            "size": 10000
                        }
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

    # 遍历各时间段内的uid
    for i in range(len(buckets)):
        sub_buckets = buckets[i]["uid"]["buckets"]
        counter = 0

        for j in range(len(sub_buckets)):
            query_body_2 = {
                "size": 0,
                "query": {
                    "filtered": {
                        "filter": {
                            "term": {
                                "uid": sub_buckets[j]["key"]
                            }
                        }
                    }
                }
            }
            response_2 = es.search(
                index = index_name_for_uid_media_mapping,
                doc_type = event_name_for_uid_media_mapping,
                body = query_body_2)

            # 如果uid==1(表示媒体)，则计数器+1
            if response_2["hits"]["total"] == 1:
                counter = counter + 1
        num_of_blog_media.append(counter)

    return num_of_blog_media


def source_count(index_name, event_name, es, field_name):

    start_index = 0
    end_index = 0
    temp_list = []

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggs": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                },
                "aggs": {
                    "source_count": {
                        "terms": {
                            "field": field_name,
                            "size": 100000
                        }
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
        d = dict()
        d["timestamp"] = buckets[i]["key"]
        d["num_of_source"] = len(buckets[i]["source_count"]["buckets"])
        temp_list.append(d)

    # 截取有效内容
    for i in range(len(temp_list)):
        if temp_list[i]["timestamp"] == start_timestamp:
            start_index = i
        if temp_list[i]["timestamp"] == end_timestamp:
            end_index = i

    end_index = end_index + 1  # 截取区间左闭右开
    news_source_count = temp_list[start_index:end_index]

    return news_source_count


def calculate_num_of_media(blog_media, baidu_media, wechat_media):

    total_num_of_media = []

    for i in range(len(blog_media)):
        total = blog_media[i] + baidu_media[i]["num_of_source"] + wechat_media[i]["num_of_source"]
        total_num_of_media.append(total)

    return total_num_of_media


# 舆情关注度
def get_num_of_post(index_name, event_name, es):

    # 微博帖子数量
    num_of_blog = get_num_of_blog(index_name, event_name, es)
    # 百度新闻数量
    num_of_baidu_news = get_num_of_article(index_name_for_baidu_news, event_name, es)
    # 微信文章数量
    num_of_wechat_article = get_num_of_article(index_name_for_wechat_data, event_name, es)
    # 求和
    num_of_post = calculate_num_of_post(num_of_blog, num_of_baidu_news, num_of_wechat_article)

    # for i in range(35):
    #     print num_of_blog[i], num_of_baidu_news[i]["count"], num_of_wechat_article[i]["count"]
    # print num_of_post

    return num_of_post


def  get_num_of_blog(index_name, event_name, es):

    num_of_blog = []

    # 各时间段帖子的数量
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


def get_num_of_article(index_name, event_name, es):

    start_index = 0
    end_index = 0
    temp_list = []

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggregations": {
            "daily_articles": {
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

    buckets = response["aggregations"]["daily_articles"]["buckets"]

    for i in range(len(buckets)):
        d = dict()
        d["timestamp"] = buckets[i]["key"]
        d["count"] = buckets[i]["doc_count"]
        temp_list.append(d)

    # 截取有效内容
    for i in range(len(temp_list)):
        if temp_list[i]["timestamp"] == start_timestamp:
            start_index = i
        if temp_list[i]["timestamp"] == end_timestamp:
            end_index = i

    end_index = end_index + 1   # 截取区间左闭右开
    result_list = temp_list[start_index:end_index]

    # for i in range(len(buckets)):
    #     print result_list[i]

    return result_list


def calculate_num_of_post(blog, baidu_news, wechat_article):

    num_of_post = []

    for i in range(len(blog)):
        total = blog[i] + baidu_news[i]["count"] + wechat_article[i]["count"]
        num_of_post.append(total)

    return num_of_post


# 地区覆盖度
def get_num_of_area(index_name, event_name, es):

    num_of_area = []

    # 先按时间进行聚合，再按地区进行聚合
    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggs": {
            "time_slice": {
                "histogram": {
                    "field": "timestamp",
                    "interval": time_slice,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start_timestamp,
                        "max": end_timestamp
                    }
                },
                "aggs": {
                    "area": {
                        "terms": {
                            "field": "geo",
                            "size": 100000
                        }
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
        sub_buckets = buckets[i]["area"]["buckets"]
        num_of_area.append(len(sub_buckets))

    return num_of_area


# 事件持续时间
def get_specific_day(index_name, event_name, es):

    # 0表示没有达到目标天数，1表示达到目标天数
    target_timestamp = 0

    query_body = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggregations": {
            "duration": {
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

    buckets = response["aggregations"]["duration"]["buckets"]

    if len(buckets) < length_of_duration:
        return target_timestamp
    else:
        target_timestamp = 1
        return target_timestamp


# 媒体扩散速度
def get_media_propagation_speed(media):

    propagation_rate = []

    for i in range(len(media)):
        if i == 0:
            propagation_rate.append(0)
        else:
            rate = (media[i] - media[i-1])
            propagation_rate.append(rate)

    return propagation_rate


def get_heat_propagation_speed(post):

    propagation_rate = []

    for i in range(len(post)):
        if i == 0:
            propagation_rate.append(0)
        else:
            rate = (post[i] - post[i-1])
            propagation_rate.append(rate)

    return propagation_rate


def get_area_propagation_speed(area):

    propagation_rate = []

    for i in range(len(area)):
        if i == 0:
            propagation_rate.append(0)
        else:
            rate = (area[i] - area[i - 1])
            propagation_rate.append(rate)

    return propagation_rate


def get_num_of_foreign_data(event_name, es):

    # facebook帖子数量
    query_for_facebook = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggregations": {
            "daily": {
                "histogram": {
                    "field": "post_publish_timestamp",
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
    fb_response = es.search(
        index = index_name_for_facebook_data,
        doc_type = event_name,
        body = query_for_facebook)
    buckets = fb_response['aggregations']['daily']['buckets']

    num_of_fb_post = []
    for i in range(len(buckets)):
        # 只截取相关时间段内的数据
        if buckets[i]['key'] < start_timestamp:
            continue
        elif buckets[i]['key'] > end_timestamp:
            break
        else:
            num_of_fb_post.append(buckets[i]['doc_count'])

    # twitter帖子数量
    query_for_twitter = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggregations": {
            "daily": {
                "histogram": {
                    "field": "post_publish_timestamp",
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
    tt_response = es.search(
        index = index_name_for_twitter_data,
        doc_type = event_name,
        body = query_for_twitter)
    buckets = tt_response['aggregations']['daily']['buckets']

    num_of_tt_post = []
    for i in range(len(buckets)):
        # 只截取相关时间段内的数据
        if buckets[i]['key'] < start_timestamp:
            continue
        elif buckets[i]['key'] > end_timestamp:
            break
        else:
            num_of_tt_post.append(buckets[i]['doc_count'])

    # ftp帖子数量
    query_for_ftp = {
        "size": 0,
        "query": {
            "match_all": {}
        },
        "aggregations": {
            "daily": {
                "histogram": {
                    "field": "post_publish_timestamp",
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
    ftp_response = es.search(
        index=index_name_for_ftp_data,
        doc_type=event_name,
        body=query_for_ftp)
    buckets = ftp_response['aggregations']['daily']['buckets']

    num_of_ftp_post = []
    for i in range(len(buckets)):
        # 只截取相关时间段内的数据
        if buckets[i]['key'] < start_timestamp:
            continue
        elif buckets[i]['key'] > end_timestamp:
            break
        else:
            num_of_ftp_post.append(buckets[i]['doc_count'])

    return num_of_fb_post, num_of_tt_post, num_of_ftp_post


def macro_interface(index_name, event_name, es):
    '''
    宏观风险包括：
    A：媒体覆盖数量
    B：舆情关注度
    C：地区覆盖数量
    N：境外媒体关注度

    D：时间持续时间
    E：媒体扩散速度
    F：热度扩散速度
    G：区域扩散速度
    '''

    initialization(index_name, event_name, es)

    # 媒体数量
    num_of_media = get_num_of_media(index_name, event_name, es)
    # 帖子数量
    num_of_post = get_num_of_post(index_name, event_name, es)
    # 地区数量
    num_of_area = get_num_of_area(index_name, event_name, es)
    # 境外媒体帖子数量
    num_of_fb, num_of_tt, num_of_ftp = get_num_of_foreign_data(event_name, es)
    # 事件持续时间
    target_timestamp = get_specific_day(index_name, event_name, es)

    # 媒体传播速度
    media_propagation_rate = get_media_propagation_speed(num_of_media)
    # 帖子数量变化速度
    heat_propagation_rate = get_heat_propagation_speed(num_of_post)
    # 地区数量变化速度
    area_propagation_rate = get_area_propagation_speed(num_of_area)

    return num_of_media, num_of_post, num_of_area, target_timestamp, \
           media_propagation_rate, heat_propagation_rate, area_propagation_rate, \
           num_of_fb, num_of_tt, num_of_ftp


if __name__ == '__main__':

    index_name = "flow_text_data"
    event_name = "lala"
    es = Elasticsearch(['219.224.134.226:9207'])

    st = time.time()
    macro_interface(index_name, event_name, es)
    et = time.time()

    print "running time: ", et - st