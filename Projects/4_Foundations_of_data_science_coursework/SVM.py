import time

from sklearn import svm
from sklearn.metrics import roc_auc_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.preprocessing import StandardScaler


def linear_svm(train_data, train_label, test_data, test_label, c_value):

    print(c_value)
    classifier = svm.LinearSVC(C=c_value, loss='hinge' , dual=False, max_iter=200000, class_weight='balanced')  #class_weight='balanced'
    # 基于liblinear库实现，适用于大样本&稀疏样本
    # 关于参数class_weight与不平衡样本：https://blog.csdn.net/gracejpw/article/details/103054668
    classifier.fit(train_data, train_label.ravel())  # ravel函数在降维时默认是行序优先

    label_hat = classifier.predict(test_data)

    accuracy = accuracy_score(test_label, label_hat)
    precision = precision_score(test_label, label_hat)
    recall = recall_score(test_label, label_hat)
    f1 = f1_score(test_label, label_hat)
    # auc = roc_auc_score(test_label, label_hat, average='macro')

    print("LinearSVC accuracy score:", accuracy)
    print("LinearSVC precision score:", precision)
    print("LinearSVC recall score:", recall)
    print("LinearSVC f1 score:", f1)
    # print("LinearSVC Roc AUC score:", auc)

    score_list = []
    score_list.append(accuracy)
    score_list.append(precision)
    score_list.append(recall)
    score_list.append(f1)
    # score_list.append(auc)

    return score_list


def svm_function(train_data, train_label, test_data, test_label):

    classifier = svm.SVC(C=0.8, kernel='linear', max_iter=300000, class_weight='balanced') # 基于libsvm库实现
    classifier.fit(train_data, train_label.ravel())  # ravel函数在降维时默认是行序优先

    label_hat = classifier.predict(test_data)
    # print(label_hat)

    print("SVC accuracy score:", accuracy_score(test_label, label_hat))
    print("SVC precision score:", precision_score(test_label, label_hat))
    print("SVC recall score:", recall_score(test_label, label_hat))
    print("SVC f1 score:", f1_score(test_label, label_hat))
    print("SVC Roc AUC score:", roc_auc_score(test_label, label_hat , average='macro'))

    return 0

# 可以通过 model.support_vectors_ 查看支持向量
# SVM 对特征的缩放非常敏感
# 当C越大时，对进入边界的数据惩罚越大，表现为进入分类边界的数据越少
# C值的确定与问题有关，如医疗模型或垃圾邮件分类问题
# 默认的损失函数为合页损失函数（hinge loss function）
# 常用的核函数由：线性核、多项式核、高斯RBF核、Sigmoid核
# gamma越大，越可能过拟合； gamma越小，越可能欠拟合
# LinearSVC类会对偏执项进行正则化，所以需要先减去平均值，使训练集集中。如果使用StandardScaler会自动进行这一步
# dual=False,除非特征数量比训练实例还多，否则应设为 False
# 与Logistic回归分类器不同的是，SVM分类器不会输出每个类别的概率

# 寻找正确的超参数值的常用方法是网络搜索。先进行一次粗略的网络搜索，然后在最好的值附近展开一轮更精细的网络搜索，这样通常会快一些
# StandardScaler(copy=True, with_mean=True, with_std=True)
# SVC(kernel="poly", degree=3, coef0=1, C=5) d和coe变大会过拟合

# sklearn.svm.LinearSVR（训练数据需要先缩放并集中）


