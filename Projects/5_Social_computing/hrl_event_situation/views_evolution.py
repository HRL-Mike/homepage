# -*- coding:utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import json
from flask import Blueprint, render_template, request
from risk_assessment_module_output import curve_output_for_frontend
from risk_assessment_module_output import content_output_for_frontend

from global_utils import es_data_text


reload(sys)
sys.setdefaultencoding("utf-8")

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('evolution_analysis', __name__, url_prefix='/evolution')   # url_prefix = '/test'  增加相对路径的前缀


# http://219.224.134.220:9999/evolution/curve_output?event_name=gangdu
@mod.route('/curve_output', methods=['POST','GET'])
def curve_output():

    event_name = request.args.get('event_name')
    print "-----------------------------"
    print es_data_text
    d = curve_output_for_frontend(event_name, es_data_text)
    d = json.dumps({"timestamp": [1553904000, 1553990400, 1554076800, 1554163200, 1554249600, 1554336000, 1554422400, 1554508800, 1554595200, 1554681600], "prediction": [1198.3], "heat_and_emotion": [{"num_of_blog": 74, "num_of_wechat_article": 0, "total_heat": 8, "negative_percent": 0, "num_of_baidu_news": 0}, {"num_of_blog": 494, "num_of_wechat_article": 0, "total_heat": 53, "negative_percent": 1, "num_of_baidu_news": 0}, {"num_of_blog": 10714, "num_of_wechat_article": 0, "total_heat": 1519, "negative_percent": 34, "num_of_baidu_news": 0}, {"num_of_blog": 16898, "num_of_wechat_article": 0, "total_heat": 2102, "negative_percent": 31, "num_of_baidu_news": 0}, {"num_of_blog": 9424, "num_of_wechat_article": 0, "total_heat": 1097, "negative_percent": 19, "num_of_baidu_news": 0}, {"num_of_blog": 9298, "num_of_wechat_article": 0, "total_heat": 1057, "negative_percent": 10, "num_of_baidu_news": 0}, {"num_of_blog": 5731, "num_of_wechat_article": 0, "total_heat": 737, "negative_percent": 26, "num_of_baidu_news": 0}, {"num_of_blog": 5851, "num_of_wechat_article": 0, "total_heat": 660, "negative_percent": 7, "num_of_baidu_news": 0}, {"num_of_blog": 3285, "num_of_wechat_article": 0, "total_heat": 381, "negative_percent": 3, "num_of_baidu_news": 0}], "event_index": [37.7, 292.0, 5099.0, 5544.5, 2401.1, 2460.5, 1508.9, 1668.0, 957.6], "evolution_and_key_user": [{"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 65}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 432}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 7799}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 12782}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 7337}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 6979}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 4582}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 4359}, {"num_of_news_source": 0, "num_of_article_source": 0, "num_of_blog_participant": 2531}], "stage": [0, 1, 2, 3, 0, 0, 0, 0, 0, 0]})
    print("d",d)
    return d

# http://219.224.134.220:9999/evolution/risk_details_output?event_name=gangdu&timestamp=1525651200&page_number=1&page_size=5
@mod.route('/risk_details_output', methods=['POST','GET'])
def risk_details_output():

    event_name = request.args.get('event_name')
    timestamp = request.args.get('timestamp')
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')

    content_result = content_output_for_frontend(event_name, timestamp, 
                                                 page_number, page_size, es_data_text)  
    
    return json.dumps(content_result)