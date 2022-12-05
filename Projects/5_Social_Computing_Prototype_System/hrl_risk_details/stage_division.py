# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from global_utils import stage_division_evolution_to_climax


def risk_stage_division(event_index):
    '''
    阶段划分算法
    包含4个阶段：出现，发酵，高潮，平息
    '''

    stage = []
    max_length = len(event_index)
    max_index = max(event_index)
    switch_1 = 0
    switch_2 = 1
    switch_3 = 1
    for i in range(len(event_index)):
        if i == 0:  # i == 0
            stage.append(0)
        elif i+2 < max_length and i > 0:  # last > i > 0
            if  event_index[i+1] > 3*event_index[i] and 1.2*event_index[i+2] > event_index[i+1] and switch_1 == 0:  # 只看后两个点
                stage.append(1)
                switch_1 = 1  # 只标记第一个满足条件的点
                switch_2 = 0
            elif event_index[i+1] > event_index[i] and 1.2*event_index[i] > event_index[i-1] and event_index[i+1] > 0.6*max_index and event_index[i+1] > stage_division_evolution_to_climax and switch_2 == 0:
                stage.append(2)
                switch_2 = 1
                switch_3 = 0
            elif event_index[i+1] < event_index[i] and event_index[i] < event_index[i-1] and event_index[i+1] < 0.2*max_index and event_index[i+1] > 0.05*max_index and switch_3 == 0:
                stage.append(3)
                switch_3 = 1
                switch_2 = 0
            else:
                stage.append(0)
        elif i+1 == max_length or i+2 == max_length:  # i == last
            stage.append(0)

    return stage


if __name__ == '__main__':

    event_index = [33,35,12,9,20,90,455,191,158,169,164,153]
    event_index2 = [33,35,12,9,20,90,455,191,158,169,164,153,2300,511,276,233,245,3600,677,512,233]

    stage = risk_stage_division(event_index)
    print event_index
    print stage
    print

    stage = risk_stage_division(event_index2)
    print event_index2
    print stage
    print

    event_index3 = [30, 35, 40, 59, 60, 90, 280, 375, 486, 591, 666, 889]
    event_index4 = [30, 35, 40, 59, 60, 90, 280, 260, 486, 591, 666, 889,1080,600,400,300,200,100]

    stage = risk_stage_division(event_index3)
    print event_index3
    print stage
    print

    stage = risk_stage_division(event_index4)
    print event_index4
    print stage
