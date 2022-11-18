# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from sklearn.ensemble import RandomForestRegressor

from global_utils import num_of_prediction


def get_data(event_index):

    train_x = []
    train_y = event_index
    for j in range(len(event_index)):
        train_x.append(j+1)

    return train_x, train_y


def predict(train_x, train_y):
    '''
    用前n天的事件指数预测未来1天的事件指数
    '''

    x_array = np.array(train_x).reshape(-1,1)
    y_array = np.array(train_y).reshape(-1,1).ravel()
    # print x_array
    # print y_array

    test = []
    test.append(len(train_x)+1)
    x_test = np.array(test).reshape(-1, 1)

    # 随机森林回归模型
    forest_reg = RandomForestRegressor(n_estimators=20, random_state=1729)
    forest_reg.fit(x_array, y_array)

    predict = forest_reg.predict(x_test)
    result = round(predict[0], 1)

    return result


def prediction_interface(event_index):

    train_x, train_y = get_data(event_index)

    for i in range(num_of_prediction):
        predict_result = predict(train_x, train_y)
        if predict_result > 0:
            event_index.append(predict_result)  # 只保留大于0的结果
        else:
            event_index.append(0)
        train_x, train_y = get_data(event_index)

    return event_index


if __name__ == '__main__':

    event_index = [31.14, 33.2, 11.2, 5.9, 20.4, 93.1, 423.5, 199.4, 176.9, 209.8, 179.8, 118.06, 149.48, 293.7,
                   208.4, 144.9, 191.4, 147.0, 152.7, 150.0, 1779.6, 406.2, 213.2, 177.8, 207.2, 531.1, 209.3, 176.1,
                   288.3, 665.5, 400.6, 289.88, 261.71, 186.4, 372.5]

    prediction_interface(event_index)