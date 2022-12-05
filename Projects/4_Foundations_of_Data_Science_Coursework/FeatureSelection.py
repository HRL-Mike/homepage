import pandas as pd
import numpy as np

from sklearn.decomposition import PCA
from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LassoCV


def reduce_dimension_pca(train_data, test_data):

    # 创建pca类的对象
    pca = PCA(n_components=10, random_state=10)  # 降至6维
    '''PCA将高维空间中的数据投影到低维空间，投影后的特征与原特征没有直接对应关系'''

    new_train_data = pca.fit_transform(train_data)  # 等价于pca.fit(X), pca.transform(X)
    # inv_ds = pca.inverse_transform(new_ds)  # 将降维后的数据转换成原始数据

    # print('\n', pca.explained_variance_ratio_)  # 降维后的各主成分的方差值占总方差值的比例,这个比例越大，则越是重要的主成分
    # 结果显示前六个特征占所有特征的方差百分比为99.6%,即保留了绝大部分信息


    new_test_data = pca.transform(test_data)  # 对测试集数据进行降维


    return new_train_data, new_test_data


def feature_selection(train_data, train_label, test_data):

    # train_data = pd.DataFrame(train_data, dtype=np.float32)
    # test_data = pd.DataFrame(test_data, dtype=np.float32)

    classifier = RandomForestClassifier(n_estimators=100, random_state=1729)
    estimator = classifier.fit(train_data, train_label)
    # print(estimator.feature_importances_)
    feature = SelectFromModel(estimator, prefit=True)
    X_train = feature.transform(train_data)
    X_test = feature.transform(test_data)
    print('After feature selection, train_data shape:', X_train.shape, '\n')

    return X_train, X_test
