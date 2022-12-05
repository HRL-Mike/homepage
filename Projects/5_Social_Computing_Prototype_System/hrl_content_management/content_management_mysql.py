# -*- coding: utf-8 -*-

import pymysql
import re

from global_utils import MySQL_host_ip
from global_utils import MySQL_host_port
from global_utils import MySQL_user
from global_utils import MySQL_psd
from global_utils import db_name


# 不使用此模块

def add_new_item(table_name, content_list):  # 添加

    db = pymysql.connect(host=MySQL_host_ip, port=MySQL_host_port, user=MySQL_user,
                         passwd=MySQL_psd, db=db_name, charset="utf8")
    cursor = db.cursor()

    input_list = []
    # if re.search('.*_table', table_name, flags=0):
    for i in range(len(content_list)):
        input_list.append((content_list[i][0], content_list[i][1]))   # 0是name 1是uid
    sql_insert = "insert into {table} values (%s, %s)".format(table = table_name)

    try:
        cursor.executemany(sql_insert, input_list)
        db.commit()
        print 'commit successfully'
        db.close()
        return True
    except:
        db.rollback()
        print 'rollback'

    db.close()
    return False


def delete_item(table_name, uid):
    db = pymysql.connect(host=MySQL_host_ip, port=MySQL_host_port, user=MySQL_user,
                         passwd=MySQL_psd, db=db_name, charset="utf8")
    cursor = db.cursor()

    sql_delete = "delete from {table} where uid={value}".format(table=table_name, value=uid)

    try:
        cursor.execute(sql_delete)
        db.commit()
        print 'commit successfully'
        db.close()
        return True
    except:
        db.rollback()
        print 'rollback'

    db.close()
    return False


def query(table_name, page_size, page_number):

    start_from = (int(page_number) - 1) * int(page_size)
    end_at = int(start_from) + int(page_size)

    db = pymysql.connect(host=MySQL_host_ip, port=MySQL_host_port, user=MySQL_user,
                         passwd=MySQL_psd, db=db_name, charset="utf8")
    cursor = db.cursor()

    sql_select = "select * from {table}".format(table=table_name)

    result_list = []
    try:
        cursor.execute(sql_select)
        result = cursor.fetchall()
        temp_list = list(result)
        result_list = temp_list[start_from:end_at]
    except:
        print "Error: query {table} failed".format(table=table_name)
    db.close()

    return result_list


if __name__ == '__main__':

    # table_name = 'big_v_table'
    # content_list = [['wqwc', 1234557890], ['agqwec', 1244567899]]
    # add_new_item(table_name, content_list)

    # table = 'big_v_table'
    # item = 2006455031
    # delete_item(table, item)

    table = 'big_v_table'
    result = query(table)


