import time
import pandas as pd

from Preprocess import process_train_set, under_sample, standardization, normalization, split_dataset
from FeatureSelection import reduce_dimension_pca, feature_selection
from SVM import svm_function
from SVM import linear_svm
from Plot import plot


def run(train_data_path, test_data_path):

    time_list = []

    # 读取数据集
    train_ds = pd.read_csv(train_data_path)

    # 预处理
    train_ds = process_train_set(train_ds)  #删除各种无用的列

    #实验1
    y_axis_value = []
    num_of_neg_sample = [5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000]
    # c_list = [0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6]
    c_value = 0.8
    for num in num_of_neg_sample:
        sample = under_sample(train_ds, num)  #欠采样
        sample_standardized = standardization(sample)  #标准化
        # sample_normalized = normalization(sample)  #归一化，二选一

        train_data, test_data, train_label, test_label = split_dataset(sample_standardized)  #划分训练集和测试集

        #降维
        # reduced_train_data, reduced_test_data = reduce_dimension_pca(train_data, test_data)
        X_train, X_test = feature_selection(train_data, train_label, test_data)

        #训练SVM模型
        time_start = time.time()
        score_list = linear_svm(X_train, train_label, X_test, test_label, c_value)
        time_end = time.time()
        y_axis_value.append(score_list)
        time_list.append(time_end-time_start)

    print(y_axis_value)
    print('time cost:',time_list,'s')
    plot(num_of_neg_sample, y_axis_value)


if __name__ == "__main__":

    # 数据集路径
    train_data_path = "C:\\Users\\herunlong\\Desktop\\Coursework\\train.csv"
    test_data_path = "C:\\Users\\herunlong\\Desktop\\Coursework\\test.csv"

    run(train_data_path, test_data_path)