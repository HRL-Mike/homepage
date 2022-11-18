#-*- coding:utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Blueprint, render_template, request
from risk_point_module_output import curve_output_interface
from risk_point_module_output import content_output_interface

from global_utils import es_data_text

reload(sys)
sys.setdefaultencoding("utf-8")

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('risk_point', __name__, url_prefix='/risk')   # url_prefix = '/test'  增加相对路径的前缀


# http://219.224.134.220:9999/risk/risk_point_curve?event_name=gangdu
@mod.route('/risk_point_curve', methods=['POST','GET'])
def risk_point_curve():

    event_name = request.args.get('event_name')
    result_in_json = curve_output_interface(event_name, es_data_text)

    return result_in_json

# http://219.224.134.220:9999/risk/risk_point_details?event_name=gangdu&timestamp=1525651200&curve_name=sensitive_risk&page_number=1&page_size=5
@mod.route('/risk_point_details', methods=['POST','GET'])
def risk_point_details():

    event_name = request.args.get('event_name')
    timestamp = request.args.get('timestamp')
    curve_name = request.args.get('curve_name') # 需要字符串参数 如 "sensitive_risk"
    page_number = request.args.get('page_number')
    page_size = request.args.get('page_size')

    result_in_json = content_output_interface(event_name, timestamp, curve_name, 
                                              page_number, page_size, es_data_text)

    return result_in_json
