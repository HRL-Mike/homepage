#-*- coding:utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Blueprint, render_template, request
from subevent_analysis_module_event_index_output import subevent_index_interface
from subevent_analysis_module_word_frequency_output import subevent_word_frequency_interface

from elasticsearch import Elasticsearch
from global_utils import es_data_text


reload(sys)
sys.setdefaultencoding("utf-8")

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('subevent_analysis', __name__, url_prefix='/subevent')   # url_prefix = '/test'  增加相对路径的前缀


# http://219.224.134.220:9999/subevent/subevent_index_output?events=taidu_zhanzhong
@mod.route('/subevent_index_output', methods=['POST','GET'])
def subevent_index_output():

    events = request.args.get('events')
    result_dict = subevent_index_interface(events, es_data_text)  # events='taidu_zhanzhong' 原来叫combination_name
    print 1111
    return result_dict


# http://219.224.134.220:9999/subevent/subevent_word_output?events=taidu_zhanzhong
@mod.route('/subevent_word_output', methods=['POST','GET'])
def subevent_word_output():

    events = request.args.get('events')   # events='台独_占中_港独'
    date = request.args.get('date')  # _id
    result_dict = subevent_word_frequency_interface(events, date, es_data_text)

    return result_dict