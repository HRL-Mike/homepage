import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from imblearn.ensemble import EasyEnsemble, EasyEnsembleClassifier
from imblearn.under_sampling import RandomUnderSampler
from sklearn.utils import shuffle


def remove_constant_col(dataset):
    '''
    删除常值列
    '''
    removed = []
    for col in dataset.columns:
        if dataset[col].std() == 0:
            removed.append(col)
    dataset.drop(removed, axis=1, inplace=True)  # axis=1表示删除列，inplace=True表示修改原数组
    print('After removing constant columns:', dataset.shape)

    return dataset


def remove_useless_col(dataset):
    '''
    删除非0元素少于20的列
    '''
    num_non_zero = np.sum(dataset!=0, axis=0).sort_values(ascending=True)
    num_non_zero_col = list(num_non_zero.index[num_non_zero < 20])

    dataset.drop(num_non_zero_col, axis=1, inplace=True)
    print('After removing useless columns:', dataset.shape)

    return dataset


def remove_duplicate_col(dataset):
    '''
    删除重复的列
    '''
    duplicate_col = []
    cols = dataset.columns

    for i in range(len(cols)-1):
        v = dataset[cols[i]].values
        for j in range(i + 1, len(cols)):
            if np.array_equal(v, dataset[cols[j]].values):
                if cols[j] not in duplicate_col:
                    duplicate_col.append(cols[j])
    dataset.drop(duplicate_col, axis=1, inplace=True)
    print('After removing duplicate columns:', dataset.shape)

    return dataset


def split_dataset(dataset):

    train_set = dataset.drop(["TARGET"], axis=1)
    label = dataset.TARGET.values

    train_data, test_data, train_label, test_label = train_test_split(train_set, label, test_size=0.2, random_state=1729)
    print("train_data shape is", train_data.shape, "\ntest_data shape is", test_data.shape)
    print("train_label shape is", train_label.shape, "\ntest_label shape is", test_label.shape, '\n')

    # print(test_label.sum())
    # print(train_label.sum())

    return train_data, test_data, train_label, test_label


def process_train_set(train_ds):

    print('Pre-process the training set:')
    train_ds = remove_constant_col(train_ds)
    train_ds = remove_useless_col(train_ds)
    train_ds = remove_duplicate_col(train_ds)
    print('Done.', '\n')

    return train_ds


def ensemble(dataset):

    train_set = dataset.drop(["TARGET", "ID"], axis=1)
    label = dataset.TARGET.values
    print(train_set.shape)

    ee = EasyEnsemble(random_state=10, n_subsets=10)

    X_resampled, y_resampled = ee.fit_sample(train_set, label)

    return X_resampled, y_resampled


def under_sample(train_set, num_of_neg_sample):

    # rus = RandomUnderSampler(sampling_strategy={0:3008, 1:3008}, random_state=42)
    # X_resampled, y_resampled = rus.fit_resample(train_set, label) # 正例:负例=1:1

    pos_sample = train_set[train_set['TARGET']==1]
    neg_sample = train_set[train_set['TARGET']==0]

    # print(pos_sample.shape) #3008
    neg_sample_shuffled = shuffle(neg_sample, random_state=20)

    # print(neg_sample.head(8),'\n')
    # print(neg_sample_shuffled.head(8))

    neg_sample_target = neg_sample_shuffled.head(num_of_neg_sample)

    # print(neg_sample_target.shape) #3008

    sample = pd.concat([neg_sample_target, pos_sample], axis=0)  #拼接两个df
    # print(sample.head(5))
    # print(sample.tail(5))
    sample_2 = shuffle(sample, random_state=42)
    print(sample.shape)

    return sample_2


def standardization(dataset):

    # print(dataset.head(5))
    train_set = dataset.drop(["TARGET", "ID"], axis=1)
    label = dataset.TARGET.values
    # print(train_set.head(5))

    train_set = (train_set - train_set.mean()) / (train_set.std())  #z-score标准化
    # print(train_set.head(5))

    train_set['TARGET'] = label
    # print(train_set.head(5))

    return train_set


def normalization(dataset):

    # print(dataset.head(5))
    train_set = dataset.drop(["TARGET", "ID"], axis=1)
    label = dataset.TARGET.values
    # print(train_set.head(5))

    train_set = (train_set - train_set.min()) / (train_set.max()-train_set.min())  #归一化
    # print(train_set.head(5))

    train_set['TARGET'] = label
    # print(train_set.head(5))

    return train_set


if __name__ == "__main__":

    # 数据集路径
    train_data_path = "C:\\Users\\herunlong\\\Desktop\\Coursework\\train.csv"
    test_data_path = "C:\\Users\\herunlong\\\Desktop\\Coursework\\test.csv"

    train_ds = pd.read_csv(train_data_path)

    train_ds = process_train_set(train_ds)
    under_sample(train_ds)