import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def data_exploration(dataset):

    # 训练集基本信息
    print('shape: {}'.format(dataset.shape))
    # print(dataset['var15'].describe())

    # 缺失数据
    print('Num of nan in dataset:', sum(dataset.isnull().sum()))
    # isnull().sum() 将列中为空的个数统计出来
    # print(train.isnull().sum(axis=0).sort_values(ascending=False).head(10))

    # 常值列 (包括0和非0)
    constant_col = []
    for col in dataset.columns:
        if dataset[col].std() == 0:
            constant_col.append(col)
    print('Num of constant columns:', len(constant_col))
    # print('Constant cloumns:', constant_col)

    # 几乎全是0的列
    num_non_zero = np.sum(dataset!=0, axis=0).sort_values(ascending=True)  # 各列不为0的元素数量
    num_non_zero_col = list(num_non_zero.index[num_non_zero < 20])
    print('Num of almost all zeros columns:', len(num_non_zero_col))
    # print('Almost all zeros columns: ', num_non_zero_col)

    # 重复列
    duplicate_col = []
    cols = dataset.columns

    for i in range(len(cols)-1):
        v = dataset[cols[i]].values
        for j in range(i+1, len(cols)):
            if np.array_equal(v, dataset[cols[j]].values):
                if cols[j] not in duplicate_col:
                    duplicate_col.append(cols[j])
    print('Num of duplicate columns:', len(duplicate_col), '\n')
    # print('Duplicate columns:', duplicate_col)

    return 0


def label_exploration(train_ds):

    print(train_ds.TARGET.value_counts(), '\n')
    sns.countplot(x='TARGET', data=train_ds)  # sns.countplot(x=train['TARGET'])
    plt.show()

    df = pd.DataFrame(train_ds.TARGET.value_counts())
    df['Percentage'] = df['TARGET'] * 100 / train_ds.shape[0]
    print(df, '\n')

    return 0


if __name__ == "__main__":

    train_data_path = "C:\\Users\\herunlong\\\Desktop\\Coursework\\train.csv"
    test_data_path = "C:\\Users\\herunlong\\\Desktop\\Coursework\\test.csv"

    train_ds = pd.read_csv(train_data_path)
    test_ds = pd.read_csv(test_data_path)

    print('Training set info:')
    data_exploration(train_ds)
    print('Labels in training set:')
    label_exploration(train_ds)

    print('Test set info:')
    data_exploration(test_ds)