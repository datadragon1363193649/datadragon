# -*- encoding: utf-8 -*-
import lime.lime_tabular
from lime.lime_tabular import LimeTabularExplainer
import pandas as pd
import sklearn
import sklearn.datasets
import sklearn.ensemble
import numpy as np
from sklearn.metrics import accuracy_score
from sklearn import cross_validation
from sklearn.externals import joblib
import lime
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (RandomTreesEmbedding, RandomForestClassifier,
                              GradientBoostingClassifier)
import offline_db_conf as dconf
traindf = pd.read_csv('/Users/ufenqi/Documents/dataming'
                      '/integration_new/dataset/tongdun/'
                      'codenordelindclasstrain_uid_target_all_tongdun1.csv')
testdf = pd.read_csv(dconf.data_path + 'codenordelclassbad_user1.csv')
model_file = dconf.model_path + 'tongdun_model'
file_name = dconf.config_path + 'featurename_online_use_tongdun'
train_name_list = ['0', '1']
test_name_list = ['0']
with open(file_name, 'r') as fp:
    for line in fp:
        linelist = line.strip().split(',')
        train_name_list.append(linelist[1])
        test_name_list.append(linelist[1])
# print traindf.columns
traindf.columns = train_name_list
testdf.columns = test_name_list
# print traindf.columns
traindf['1'] = traindf['1'].astype('string')
cols = traindf.columns.tolist()
cols.remove('0')
cols.remove('1')

traindf[cols] = traindf[cols].astype('float')
# str_col = ['110','111','454','455','456','461','464']
X_train = traindf[cols].values
Y_train = traindf['1'].values
X_validation = testdf[cols].values

# print X_train.dtypes
model_log = LogisticRegression()
model_log.fit(X_train, Y_train)
# for ci in range(len(cols)):
#     print cols[ci],model_log.coef_[0][ci]
# print 'weight',model_log.coef_[0]
# model_log = joblib.load(model_file)
predict_fn_log = lambda  x: model_log.predict_proba(x).astype(float)
# print model_log.coef_[0]
# array = X_train.values
# print array[:5]
# number_of_features = len(array[0])
# X = array[:,0:number_of_features]
# print X[:5]
# array_train = np.array(X_train)
# array_test = np.array(X_validation)
# print array_train
# print X_train[:, '110']
explainer = LimeTabularExplainer(X_train,feature_names = cols,class_names = ['0','1'],discretize_continuous=True)

observation_1 = 0
# print array_test[observation_1]
# print predict_fn_log
# for ci in range(len(cols)):
#     print cols[ci],X_validation[observation_1][ci]
#     print traindf[cols[ci]].value_counts()
# print X_validation[observation_1]
# print 'real',model_log.predict_proba(X_validation[observation_1])[:,1]
test = [6, 0, 1, 0, 2, 8, 9, 9, 8, 9, 9, 1, 0, 1, 0, 0, 0, 0, 0, 0,]
# print 'test',model_log.predict_proba(test)[:,1]
print X_validation[observation_1]
exp = explainer.explain_instance(X_validation[observation_1], predict_fn_log, num_features=100)
# print 'pre',exp.local_pred
a = exp.as_list()
# print model_log.predict_proba(X_validation[observation_1]).astype(float)
# print exp.local_pred
sum = 0
# for li in a:
#     sum += li[1]
#     print li
# print sum + exp.intercept[1]
