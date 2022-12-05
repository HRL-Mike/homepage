
import pywt

import numpy as np
import scipy.io as sio
import matplotlib.pyplot as plt


def cwt(wave_data, sample_freq, wavelet, total_scale):
    sampling_period = 1.0 / sample_freq  # 采样周期
    fc = pywt.central_frequency(wavelet)  # 计算小波函数的中心频率
    c = 2 * fc * total_scale  # 常数c
    scales = c / np.arange(total_scale, 1, -1)  # 为使转换后的频率序列是一等差序列，尺度序列必须取为这一形式(也即小波尺度)

    [cwt_matrix, frequencies] = pywt.cwt(wave_data, scales, wavelet, sampling_period)
    return cwt_matrix, frequencies


def baseline_correction(cwt_matrix, zero_point_index):
    avg_power_list = []
    for row in cwt_matrix:
        avg_power = np.mean(row[:zero_point_index+1])
        avg_power_list.append(avg_power)
    for i, row in enumerate(cwt_matrix):
        row -= avg_power_list[i]
    return cwt_matrix


if __name__ == "__main__":
    # 读取数据
    file = './pat02_faces.mat'
    mat_data = sio.loadmat(file)

    # 提取各字段数据
    sample_freq = mat_data['data']['fsample'][0][0][0][0]  # 512
    trial_info = mat_data['data']['trialinfo'][0][0]  # <class 'numpy.ndarray'>
    sample_info = mat_data['data']['sampleinfo'][0][0]  # <class 'numpy.ndarray'>

    trial = mat_data['data']['trial'][0][0][0]  # <class 'numpy.ndarray'>, len=99, dim=3
    # print(mat_data['data']['trial'][0][0][0][98])  # <class 'numpy.ndarray'>, dim=2
    time = mat_data['data']['time'][0][0][0]  # <class 'numpy.ndarray'>, len=99, dim=3

    electrode_label = mat_data['data']['label'][0][0]  # (9, 1)
    cfg = mat_data['data']['cfg'][0][0]
    electrode_info_bipolar_label = mat_data['data']['elecinfo'][0][0][0][0][0]
    electrode_info_pos = mat_data['data']['elecinfo'][0][0][0][0][1]

    # 计算时频功率(连续小波变换)
    wave_data = trial[0][0]  # 原始信号(单被试单电极单试次)
    wavelet = 'cgau8'  # morlet小波
    total_scale = 2050  # 尺度(可以理解为频率)

    cwt_matrix, frequencies = cwt(wave_data, sample_freq, wavelet, total_scale)
    # print(cwt_matrix.shape)  # (511, 2049)

    # 基线校正
    zero_point_index = np.where(time[0][0] == 0)[0][0]  # 刺激出现时间点的下标, 768
    cwt_matrix = baseline_correction(cwt_matrix, zero_point_index)
    # print(cwt_matrix.shape)
    amp = abs(cwt_matrix)

    # 画图
    # plt.figure(figsize=(15, 12))
    #
    # plt.subplot(2, 1, 1)
    t = time[0][0]
    # plt.plot(t, wave_data)
    # plt.xlabel(u"time(s)")
    # plt.ylabel(u"amplitude(μV)")

    plt.figure(figsize=(15, 12))
    # plt.subplot(2, 1, 2)
    plt.contourf(t, frequencies, amp)  # 画等高线的函数
    plt.colorbar()
    plt.ylim(0, 35)  # 设置y轴范围
    plt.xlabel(u"time(s)")
    plt.ylabel(u"freq(Hz)")
    plt.show()

# 所有电极的时频图平均
# 复现论文里的时频图
