# -*- coding:utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from flask import Blueprint, request

# 敏感词
from content_management_sensitive_word import add_sensitive_word
from content_management_sensitive_word import query_all_for_sensitive_word
from content_management_sensitive_word import delete_sensitive_word
from content_management_sensitive_word import fuzzy_query_for_sensitive_word
# 用户列表
from content_management_user_list import add_new_item
from content_management_user_list import delete_multiple_users
from content_management_user_list import fuzzy_query_for_user_list
from content_management_user_list import query_all_for_user_list
from content_management_user_list import check_consistency
from content_management_user_list import update_user
# 权重
from content_management_weight import query_all_for_weight
from content_management_weight import modification_for_weight
# 阈值
from content_management_threshold import query_all_for_threshold
from content_management_threshold import modification_for_threshold

from global_utils import es_data_text
from global_utils import index_name_for_sensitive_word
from global_utils import index_name_for_big_v
from global_utils import index_name_for_key_media
from global_utils import index_name_for_sensitive_user
from global_utils import index_name_for_weight
from global_utils import index_name_for_threshold
from global_utils import index_name_for_sensor

reload(sys)
sys.setdefaultencoding("utf-8")

# Blueprint 模块化管理程序路由的功能
mod = Blueprint('content_management', __name__, url_prefix='/content_management')  # url_prefix = '/test'  增加相对路径的前缀


# http://219.224.134.220:9999/content_management/query_all?target=sensitive_word&page_size=5&page_number=1
@mod.route('/query_all', methods=['POST', 'GET'])
def query_all():

    page_size = request.args.get('page_size')
    page_number = request.args.get('page_number')
    target = request.args.get('target')

    start_from = (int(page_number) - 1) * int(page_size)
    end_at = int(start_from) + int(page_size)

    d = dict()
    if target == 'sensitive_word':
        sensitive_word_list, total_num = query_all_for_sensitive_word(index_name_for_sensitive_word, es_data_text)
        sensitive_word_result = sensitive_word_list[start_from:end_at]
        d['sensitive_word']={'total':total_num,'data':sensitive_word_result}
    elif target == 'big_v':
        big_v_list, total_num = query_all_for_user_list(index_name_for_big_v, es_data_text)
        big_v_result = big_v_list[start_from:end_at]
        d['big_v']={'total':total_num,'data':big_v_result}
    elif target == 'sensitive_user':
        sensitive_user_list, total_num = query_all_for_user_list(index_name_for_sensitive_user, es_data_text)
        sensitive_user_result = sensitive_user_list[start_from:end_at]
        d['sensitive_user']={'total':total_num,'data':sensitive_user_result}
    elif target == 'key_media':
        key_media_list, total_num = query_all_for_user_list(index_name_for_key_media, es_data_text)
        key_media_result = key_media_list[start_from:end_at]
        d['key_media']={'total':total_num,'data':key_media_result}
    elif target == 'sensor':
        sensor_list, total_num = query_all_for_user_list(index_name_for_sensor, es_data_text)
        sensor_result = sensor_list[start_from:end_at]
        d['sensor'] = {'total': total_num, 'data': sensor_result}
    elif target == 'weight':
        weight_list, total_num = query_all_for_weight(index_name_for_weight, es_data_text)
        weight_result = weight_list[start_from:end_at]
        d['weight']=weight_result
    elif target == 'threshold':
        threshold_list, total_num = query_all_for_threshold(index_name_for_threshold, es_data_text)
        threshold_result = threshold_list[start_from:end_at]
        d['threshold']=threshold_result
    else:
        print 'Error: unknown parameter content_name'

    return json.dumps(d)


# http://219.224.134.220:9999/content_management/check?target=big_v&content_for_checking=我是化妆女王,1682192071
@mod.route('/check', methods=['POST', 'GET'])
def check_validity():

    target = request.args.get('target')
    content_for_checking = request.args.get('content_for_checking')
    # 输入示例："我是化妆女王,1682192071"

    d = dict()
    if target == 'big_v':
        check_result = check_consistency(index_name_for_big_v, content_for_checking, es_data_text)
        d['check_result'] = check_result
        # -2: 输入的uid长度非法或包含字符，合法uid为10位数字
        # -1: uid匹配但昵称不同，询问是否覆盖
        # 0: 昵称存在，拒绝添加
        # 1: 可以添加
    elif target == 'sensitive_user':
        check_result = check_consistency(index_name_for_sensitive_user, content_for_checking, es_data_text)
        d['check_result'] = check_result
    elif target == 'key_media':
        check_result = check_consistency(index_name_for_key_media, content_for_checking, es_data_text)
        d['check_result'] = check_result
    elif target == 'sensor':
        check_result = check_consistency(index_name_for_sensor, content_for_checking, es_data_text)
        d['check_result'] = check_result
    return json.dumps(d)


# http://219.224.134.220:9999/content_management/update?target=big_v&content_for_update=我是化妆女,1682192071
@mod.route('/update', methods=['POST', 'GET'])
def update():   # check_consistency返回-1时调用此函数

    target = request.args.get('target')
    content_for_update = request.args.get('content_for_update')
    # 输入示例："我是化妆女王,1682192071"
    # channel字段总是由0更新为1

    d = dict()
    if target == 'big_v':
        update_result = update_user(index_name_for_big_v, content_for_update, es_data_text)
        d['update_result'] = update_result  # -1: 更新失败, 1: 更新成功
    elif target == 'sensitive_user':
        update_result = update_user(index_name_for_sensitive_user, content_for_update, es_data_text)
        d['update_result'] = update_result
    elif target == 'key_media':
        update_result = update_user(index_name_for_key_media, content_for_update, es_data_text)
        d['update_result'] = update_result
    elif target == 'sensor':
        update_result = update_user(index_name_for_sensor, content_for_update, es_data_text)
        d['update_result'] = update_result
    return json.dumps(d)


# http://219.224.134.220:9999/content_management/add?target=sensitive_word&content_for_adding=北京北京,南京南京
@mod.route('/add', methods=['POST', 'GET'])
def add():

    target = request.args.get('target')
    content_for_adding = request.args.get('content_for_adding')
    # 敏感词示例："南京风波,核蛋,北京事件,炸弹,北京风波,人肉炸弹"  无改动
    # 大V输入示例："我是化妆女王,1682192071"

    d = dict()
    if target == 'sensitive_word':
        add_result = add_sensitive_word(index_name_for_sensitive_word, content_for_adding.decode('utf-8'), es_data_text)
        d['add_result'] = add_result  # -1: 添加失败，输入列表中包含重复项; 0: 添加失败，未知错误或输入为空; 1: 添加成功
    elif target == 'big_v':
        add_result = add_new_item(index_name_for_big_v, content_for_adding.decode('utf-8'), es_data_text)
        d['add_result'] = add_result  # -1: 添加失败, 1: 添加成功
    elif target == 'sensitive_user':
        add_result = add_new_item(index_name_for_sensitive_user, content_for_adding.decode('utf-8'), es_data_text)
        d['add_result'] = add_result
    elif target == 'key_media':
        add_result = add_new_item(index_name_for_key_media, content_for_adding.decode('utf-8'), es_data_text)
        d['add_result'] = add_result
    elif target == 'sensor':
        add_result = add_new_item(index_name_for_sensor, content_for_adding.decode('utf-8'), es_data_text)
        d['add_result'] = add_result
    return json.dumps(d)


# http://219.224.134.220:9999/content_management/delete?big_v='1883818283,2065240833,1848555562'&sensitive_user=''&sensor=''&key_media=''
@mod.route('/delete', methods=['POST', 'GET'])
def delete():

    big_v = request.args.get('big_v')
    key_media = request.args.get('key_media')
    sensitive_user = request.args.get('sensitive_user')
    sensor = request.args.get('sensor')
    # 删除big_v示例: "1883818283,2065240833,1848555562"
    d = dict()
    if sensor != '':
        # 批量删除
        delete_result = delete_multiple_users(index_name_for_sensor, sensor, es_data_text)
        d['sensor'] = delete_result  # 1: 删除成功, 0: 删除失败
    if big_v != '':
        delete_result = delete_multiple_users(index_name_for_big_v, big_v, es_data_text)
        d['big_v'] = delete_result
    if sensitive_user != '':
        delete_result = delete_multiple_users(index_name_for_sensitive_user, sensitive_user, es_data_text)
        d['sensitive_user'] = delete_result
    if key_media != '':
        delete_result = delete_multiple_users(index_name_for_key_media, key_media, es_data_text)
        d['key_media'] = delete_result
    return json.dumps(d)


# http://219.224.134.220:9999/content_management/delete?sensitive_word=北京
@mod.route('/delete_word', methods=['POST', 'GET'])
def delete_word():

    sensitive_word = request.args.get('sensitive_word')  # 传入参数名已更改
    # 删除敏感词示例: "北京"
    d = dict()
    delete_result = delete_sensitive_word(index_name_for_sensitive_word, sensitive_word, es_data_text)
    d['delete_result'] = delete_result  # 1: 删除成功, 0: 删除失败
    return delete_result


# http://219.224.134.220:9999/content_management/fuzzy_query?target=sensitive_word&query_content=北京
@mod.route('/fuzzy_query', methods=['POST', 'GET'])
def fuzzy_query():

    target = request.args.get('target')
    query_content = request.args.get('query_content')
    page_size = request.args.get('page_size')
    page_number = request.args.get('page_number')

    start_from = (int(page_number) - 1) * int(page_size)
    end_at = int(start_from) + int(page_size)

    d = dict()
    if target == 'sensitive_word':
        fuzzy_query_result, total_num  = fuzzy_query_for_sensitive_word(index_name_for_sensitive_word, query_content.decode('utf-8'), es_data_text)
        final_result = fuzzy_query_result[start_from:end_at]
        d['sensitive_word']={'total':total_num, 'data':final_result}
        # 返回匹配的模糊查询结果，若返回列表为空则无匹配项
    elif target == 'big_v':
        fuzzy_query_result, total_num = fuzzy_query_for_user_list(index_name_for_big_v, query_content.decode('utf-8'), es_data_text)
        final_result = fuzzy_query_result[start_from:end_at]
        d['fuzzy_query_result']={'total':total_num,'data':final_result}
        # 输入为名字或uid均可
        # 返回列表中包含多个字典，字典中包含name&uid，若返回列表为空则无匹配项
    elif target == 'sensitive_user':
        fuzzy_query_result, total_num = fuzzy_query_for_user_list(index_name_for_sensitive_user, query_content.decode('utf-8'), es_data_text)
        final_result = fuzzy_query_result[start_from:end_at]
        d['fuzzy_query_result']={'total':total_num,'data':final_result}
    elif target == 'key_media':
        fuzzy_query_result, total_num = fuzzy_query_for_user_list(index_name_for_key_media, query_content.decode('utf-8'), es_data_text)
        final_result = fuzzy_query_result[start_from:end_at]
        d['fuzzy_query_result']={'total':total_num,'data':final_result}
    elif target == 'sensor':
        fuzzy_query_result, total_num = fuzzy_query_for_user_list(index_name_for_sensor, query_content.decode('utf-8'), es_data_text)
        final_result = fuzzy_query_result[start_from:end_at]
        d['fuzzy_query_result']={'total':total_num,'data':final_result}
    return json.dumps(d)


# http://219.224.134.220:9999/content_management/modification?target=threshold&update_name=threshold_for_num_of_media&update_value=35
@mod.route('/modification', methods=['POST', 'GET'])
def modify():

    target = request.args.get('target')
    update_name = request.args.get('update_name')
    update_value = request.args.get('update_value')

    d = dict()
    if target == 'weight':
        modification_result = modification_for_weight(index_name_for_weight, update_name.decode('utf-8'), update_value.decode('utf-8'), es_data_text)
        d['modification_result'] = modification_result
    elif target == 'threshold':
        modification_result = modification_for_threshold(index_name_for_threshold, update_name.decode('utf-8'), update_value.decode('utf-8'), es_data_text)
        d['modification_result'] = modification_result

    return json.dumps(d)