
import json

import numpy as np
import matplotlib.pyplot as plt


def draw_global_similarity(prefix, file_list, y_min, y_max):
    fig = plt.figure(figsize=(18, 6))
    for i, file_name in enumerate(file_list):
        file = prefix + file_name
        # read file
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
            content = json.loads(content)
        # plot
        fig.add_subplot(1, 3, i+1)
        x = list(range(1, 31))
        y = content
        plt.bar(x, y, width=0.4)
        plt.ylim(y_min, y_max)
        title = 'layer_' + file_name.split('_')[1] + '_global_similarity'
        plt.title(title)
        plt.xlabel('epochs')
        plt.ylabel('global_similarity')
    output_file = prefix + 'global_avg_similarity.png'
    plt.savefig(output_file)


def draw_intra_class_similarity(prefix, file_list, class_list, y_min, y_max):
    for i, file_name in enumerate(file_list):
        file = prefix + file_name
        with open(file, 'r', encoding='utf-8') as f:
            content = f.readlines()
            content = [json.loads(item) for item in content]
        arr_t = np.array(content).T  # (4, 30)
        # plot
        fig = plt.figure(figsize=(15, 15))
        for j in range(4):
            fig.add_subplot(2, 2, j+1)
            x = list(range(1, 31))
            y = arr_t[j]
            plt.bar(x, y, width=0.4)
            plt.ylim(y_min, y_max)
            title = 'layer_' + file_name.split('_')[1] + '_' + class_list[j] + '_avg_similarity'
            plt.title(title)
            plt.xlabel('epochs')
            plt.ylabel(class_list[j] + '_avg_similarity')
        output_file = prefix + 'layer_' + file_name.split('_')[1] + '_intra_class_avg_similarity.png'
        plt.savefig(output_file)


def draw_inter_class_similarity(prefix, file_list, combination_list, y_min, y_max):
    for i, file_name in enumerate(file_list):
        file = prefix + file_name
        with open(file, 'r', encoding='utf-8') as f:
            content = f.readlines()
            content = [json.loads(item) for item in content]
        arr_t = np.array(content).T  # (6, 30)
        # plot
        fig = plt.figure(figsize=(20, 15))
        for j in range(6):
            fig.add_subplot(2, 3, j+1)
            x = list(range(1, 31))
            y = arr_t[j]
            plt.bar(x, y, width=0.4)
            plt.ylim(y_min, y_max)
            title = 'layer_' + file_name.split('_')[1] + '_' + combination_list[j] + '_avg_similarity'
            plt.title(title)
            plt.xlabel('epochs')
            plt.ylabel(combination_list[j] + '_avg_similarity')
        output_file = prefix + 'layer_' + file_name.split('_')[1] + '_inter_class_avg_similarity.png'
        plt.savefig(output_file)


if __name__ == "__main__":

    prefix = r'./results/result-4/simi/'
    # 全局相似性
    y_min = 0.7
    y_max = 1
    file_list = ['layer_5_global_avg_similarity.txt', 'layer_6_global_avg_similarity.txt',
                 'layer_7_global_avg_similarity.txt']
    draw_global_similarity(prefix, file_list, y_min, y_max)
    # 类内相似性
    y_min = 0.8
    y_max = 1
    file_list = ['layer_5_intra_class_avg_similarity.txt', 'layer_6_intra_class_avg_similarity.txt',
                 'layer_7_intra_class_avg_similarity.txt']
    class_list = ['car', 'fruit', 'human', 'monkey']
    draw_intra_class_similarity(prefix, file_list, class_list, y_min, y_max)
    # 类间相似性
    y_min = 0.6
    y_max = 1
    file_list = ['layer_5_inter_class_avg_similarity.txt', 'layer_6_inter_class_avg_similarity.txt',
                 'layer_7_inter_class_avg_similarity.txt']
    combination_list = ['fruit_car', 'human_car', 'monkey_car', 'human_fruit', 'monkey_fruit', 'monkey_human']
    draw_inter_class_similarity(prefix, file_list, combination_list, y_min, y_max)
