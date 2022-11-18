
import json
import torch
import pandas as pd


def calculate_global_avg_similarity(df_matrix):
    # dataframe转ndarry
    np_matrix = df_matrix.values
    row_num = np_matrix.shape[0]  # 80
    col_num = np_matrix.shape[1]  # 80

    item_list = []
    for i in range(row_num):
        for j in range(col_num):
            if j >= i:  # 列数>=行数时,不操作
                continue
            else:
                item_list.append(np_matrix[i][j])

    length = len(item_list)  # 3160
    total = sum(item_list)
    global_avg_similarity = total / length
    return global_avg_similarity


def calculate_intra_class_avg_similarity(df_matrix):  # 类内
    np_matrix = df_matrix.values

    first_class_item_list = []
    for i in range(0, 20):
        for j in range(0, 20):
            if j >= i:  # 列数>=行数时,不操作
                continue
            else:
                first_class_item_list.append(np_matrix[i][j])
    length = len(first_class_item_list)  # 190
    total = sum(first_class_item_list)
    first_class_avg_similarity = total / length

    second_class_item_list = []
    for i in range(20, 40):
        for j in range(20, 40):
            if j >= i:  # 列数>=行数时,不操作
                continue
            else:
                second_class_item_list.append(np_matrix[i][j])
    length = len(second_class_item_list)  # 190
    total = sum(second_class_item_list)
    second_class_avg_similarity = total / length

    third_class_item_list = []
    for i in range(40, 60):
        for j in range(40, 60):
            if j >= i:  # 列数>=行数时,不操作
                continue
            else:
                third_class_item_list.append(np_matrix[i][j])
    length = len(third_class_item_list)  # 190
    total = sum(third_class_item_list)
    third_class_avg_similarity = total / length

    fourth_class_item_list = []
    for i in range(60, 80):
        for j in range(60, 80):
            if j >= i:  # 列数>=行数时,不操作
                continue
            else:
                fourth_class_item_list.append(np_matrix[i][j])
    length = len(fourth_class_item_list)  # 190
    total = sum(fourth_class_item_list)
    fourth_class_avg_similarity = total / length

    intra_class_avg_similarity = [first_class_avg_similarity, second_class_avg_similarity,
                                  third_class_avg_similarity, fourth_class_avg_similarity]
    return intra_class_avg_similarity


def calculate_inter_class_avg_similarity(df_matrix):  # 类间
    np_matrix = df_matrix.values

    # car, fruit, human, monkey
    fruit_car_item_list = []
    for i in range(20, 40):  # 行
        for j in range(0, 20):  # 列
            fruit_car_item_list.append(np_matrix[i][j])
    length = len(fruit_car_item_list)  # 400
    total = sum(fruit_car_item_list)
    fruit_car_avg_similarity = total / length

    human_car_item_list = []
    for i in range(40, 60):  # 行
        for j in range(0, 20):  # 列
            human_car_item_list.append(np_matrix[i][j])
    length = len(human_car_item_list)  # 400
    total = sum(human_car_item_list)
    human_car_avg_similarity = total / length

    monkey_car_item_list = []
    for i in range(60, 80):  # 行
        for j in range(0, 20):  # 列
            monkey_car_item_list.append(np_matrix[i][j])
    length = len(monkey_car_item_list)  # 400
    total = sum(monkey_car_item_list)
    monkey_car_avg_similarity = total / length

    human_fruit_item_list = []
    for i in range(40, 60):  # 行
        for j in range(20, 40):  # 列
            human_fruit_item_list.append(np_matrix[i][j])
    length = len(human_fruit_item_list)  # 400
    total = sum(human_fruit_item_list)
    human_fruit_avg_similarity = total / length

    monkey_fruit_item_list = []
    for i in range(60, 80):  # 行
        for j in range(20, 40):  # 列
            monkey_fruit_item_list.append(np_matrix[i][j])
    length = len(monkey_fruit_item_list)  # 400
    total = sum(monkey_fruit_item_list)
    monkey_fruit_avg_similarity = total / length

    monkey_human_item_list = []
    for i in range(60, 80):  # 行
        for j in range(40, 60):  # 列
            monkey_human_item_list.append(np_matrix[i][j])
    length = len(monkey_human_item_list)  # 400
    total = sum(monkey_human_item_list)
    monkey_human_avg_similarity = total / length

    inter_class_avg_similarity = [fruit_car_avg_similarity, human_car_avg_similarity, monkey_car_avg_similarity,
                                  human_fruit_avg_similarity, monkey_fruit_avg_similarity, monkey_human_avg_similarity]
    return inter_class_avg_similarity


if __name__ == "__main__":

    epoch_num = 30
    prefix = r'./results/result-4/'
    output_folder = 'simi/'

    layers = ['4', '5', '6', '7']  # ['5', '6', '7']
    for layer in layers:
        global_avg_similarity_list = []
        intra_class_avg_similarity_list = []
        inter_class_avg_similarity_list = []
        for i in range(epoch_num):
            # 读取数据
            epoch = 'epoch_' + str(i)
            file = prefix + epoch + '_layer_' + layer + '.txt'
            with open(file, 'r', encoding='utf-8') as f:
                content = f.readlines()
            # 处理特征向量
            vector_list = [json.loads(vec) for vec in content]
            vec_tensor_t = torch.tensor(vector_list).t()  # tensor(4096, 80)
            vec_df = pd.DataFrame(vec_tensor_t)
            vec_corr = vec_df.corr(method='spearman')  # 默认是pearson相关系数
            global_avg_similarity = calculate_global_avg_similarity(vec_corr)
            intra_class_avg_similarity = calculate_intra_class_avg_similarity(vec_corr)  # 4
            inter_class_avg_similarity = calculate_inter_class_avg_similarity(vec_corr)  # 6
            global_avg_similarity_list.append(global_avg_similarity)
            intra_class_avg_similarity_list.append(intra_class_avg_similarity)
            inter_class_avg_similarity_list.append(inter_class_avg_similarity)
        # 结果写入文件
        output_file = prefix + output_folder + 'layer_' + layer + '_global_avg_similarity.txt'  # 30个epoch的结果
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(json.dumps(global_avg_similarity_list) + '\n')
        output_file = prefix + output_folder + 'layer_' + layer + '_intra_class_avg_similarity.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in intra_class_avg_similarity_list:
                f.write(json.dumps(item) + '\n')
        output_file = prefix + output_folder + 'layer_' + layer + '_inter_class_avg_similarity.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in inter_class_avg_similarity_list:  # 6
                f.write(json.dumps(item) + '\n')
