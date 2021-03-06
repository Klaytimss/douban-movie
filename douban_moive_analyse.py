# -*- coding: utf-8 -*-
"""Douban_Moive_Analyse.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/15lQvBmH3_jEA0Uk6qY2qcfxRE4b0n6cm

***候选特征值***
* 导演
* 主演
* 编剧
* 类型
* 年代（发行时间可能有多个国家，爬取要做筛选，或者保存元数据）
* 口碑(模棱两可，困难度高，争议大，只是设想)
* 发行的月份（5-1，10-1，春节档）
* 制片国家/地区
* 豆瓣人给贴的标签
* 评价人数（小众和大众电影有差异）



**我发现一个大事儿，为什么大家一般预测票房而很少预测评分，一是商业价值，票房比评分有商业价值；2是因为好的导演电影评分不会低，而一般导演却不。**

除此之外还有很多特征可以放进去，比如微博搜索条数，比如参演人员、导演的百度新闻数。
"""



cp -r /content/drive/MyDrive/Douban/clean_data.csv /content/clean_data.csv

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

mov=pd.read_csv("/content/clean_data.csv")

print(mov.shape)
print(mov.columns.values)

mov.Durations = mov['Durations'].str.extract('(\d+)')
mov.dropna(axis=0,how='any',inplace=True)
mov.Durations = mov['Durations'].astype("int16")

mov.dropna(axis=0,how='any',inplace=True)

numeric_features=['Reviews_count', 'Durations']
text_features=['Actors', 'Directors','Writers']#, 'Genres ,
categorical_features = ['Year','DayOfYear','Country']

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

scl=StandardScaler()
mov[numeric_features]=scl.fit_transform(mov[numeric_features])
mov[numeric_features].head()

for feat in categorical_features:
    mov=pd.concat([mov, pd.get_dummies(mov[feat], prefix=feat, dummy_na=True)],axis=1)

from sklearn.feature_extraction.text import CountVectorizer
# mov["plot_keywords"]=mov["plot_keywords"].fillna("None")

def token(text):
    return(text.split("|"))

cv=CountVectorizer(tokenizer=token)
Genres=cv.fit_transform(mov["Genres"])

Genres=Genres.toarray()

words = cv.get_feature_names()
words=["Genres_"+w for w in words]

keywords=pd.DataFrame(Genres, columns=words)
print(mov.shape)
mov=pd.concat([mov, keywords],axis=1)
print(mov.shape)

def getOneCate(cate):
    mov[cate] = mov[cate].fillna("None")
    cv=CountVectorizer(tokenizer=token,max_features=4000,)
    actors=cv.fit_transform(mov[cate])

    actors=actors.toarray()

    actor_names = cv.get_feature_names()
    actor_names=[f"{cate}_"+w for w in actor_names]

    keywords=pd.DataFrame(actors, columns=actor_names)
    return keywords

for i in text_features:
    mov=pd.concat([mov, getOneCate(i)],axis=1)

print(mov.shape)



mov.dropna(axis=0,how='any',inplace=True)
X = mov.drop(["id","Reviews_count","Actors","Directors","Writers","Genres","Country","Year","DayOfYear","Languages",
              "Name"], axis = 1)

# def getStar(float_num):
#     if float_num < 3:
#         star = -1
#     elif floa_num < 7:
#         star = 0
#     else:
#         star = 1

Y = mov["Rate"].astype("int8") #.astype("int8")

Y.unique()

# 划分训练集和测试机
from sklearn.model_selection import train_test_split
# 如果你的数据是个二元的list，下面的zip反方法会很优雅，但我们的数据是dateframe，大可不必这样。
# x, y = zip(*dataset)

# 分类比例默认是25%。如果你填入一个小于1的值，它当成比例，大于1的值，它就作为数量。
x_train, x_test, y_train, y_test = train_test_split(X, Y, random_state=1,)

print(X.shape)
print(Y.shape)

from sklearn.preprocessing import StandardScaler
import sklearn.model_selection
import sklearn.linear_model
import sklearn.metrics
def KFold_score(x_train_data, y_train_data):
    fold = sklearn.model_selection.KFold(n_splits=5, shuffle=True)
    c_param_range = [0.01, 0.1, 1, 10, 100]
    result_table = pd.DataFrame(index=range(len(c_param_range)), columns=['C参数', 'recall'])
    j = 0
    for c_param in c_param_range:  # C参数，正则化强度
        recall_accs = []
        result_table.loc[j, 'C参数'] = c_param
        for train_index, valid_index in fold.split(x_train_data):  # 交叉验证:训练集 验证集
            lr = sklearn.linear_model.LogisticRegression(penalty='l1', C=c_param, solver='liblinear', max_iter=100)
            # 训练，revel()矩阵向量化
            lr.fit(x_train_data.iloc[train_index, :], y_train_data.iloc[train_index, :].values.ravel())
            y_pred = lr.predict(x_train_data.iloc[valid_index, :].values)  # 预测验证集求召回率
            recall_acc = sklearn.metrics.recall_score(y_train_data.iloc[valid_index, :], y_pred)
            recall_accs.append(recall_acc)  # 统计召回率
        result_table.loc[j, 'recall'] = np.mean(recall_accs)
        j += 1
    print(result_table, '\n')
    maxval = 0
    maxid = 0  # 求最好的C参数
    for id, val in enumerate(result_table['recall']):
        if val > maxval:
            maxval = val
            maxid = id
    best_c = result_table.loc[maxid]['C参数']
    return best_c

best_c = 1
# best_c = KFold_score(x_train, y_train)
print('C参数：', best_c, '\n')  # 得出最适C，后再进行训练
lr = sklearn.linear_model.LogisticRegression(penalty='l1', C=best_c, solver='liblinear', max_iter=1000)
lr.fit(x_train, y_train)  # 采样训练
y_pred = lr.predict(x_test.values)
cnf_matrix = sklearn.metrics.confusion_matrix(y_test, y_pred)
recall = cnf_matrix[1][1] / (cnf_matrix[1][1] + cnf_matrix[1][0])
print('recall:', recall)
precison = cnf_matrix[1][1] / (cnf_matrix[1][1] + cnf_matrix[0][1])
print('precison:', precison, '\n')