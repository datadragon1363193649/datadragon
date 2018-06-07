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
import lime
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (RandomTreesEmbedding, RandomForestClassifier,
                              GradientBoostingClassifier)
traindf = pd.read_csv('/Users/ufenqi/Documents/dataming'
                      '/integration_new/dataset/tongdun/'
                      'codenordelindclasstrain_uid_target_all_tongdun1.csv')
testdf = pd.read_csv('/Users/ufenqi/Documents/dataming'
                      '/integration_new/dataset/tongdun/'
                     'codenordelclasstest_data_9021.csv')

traindf['1']=traindf['1'].astype('string')
cols = traindf.columns.tolist()
cols.remove('0')
cols.remove('1')
# print cols
traindf[cols]=traindf[cols].astype('float')
str_col = ['110','111','454','455','456','461','464']
traindata = traindf[cols].values

X_train, X_validation, Y_train, Y_validation = \
    cross_validation.train_test_split(traindata, traindf['1'].values,
                                      test_size=0.2, random_state=7)

# print X_train.dtypes
model_log = LogisticRegression()
model_log.fit(X_train, Y_train)

model_rf = RandomForestClassifier()
model_rf.fit(X_train, Y_train)
predict_fn_log = lambda  x: model_log.predict_proba(x).astype(float)
predict_fn_rf = lambda  x: model_rf.predict_proba(x).astype(float)
print accuracy_score(Y_validation, model_log.predict(X_validation))
print accuracy_score(Y_validation, model_rf.predict(X_validation))
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

observation_1 = 2
# print array_test[observation_1]
# print predict_fn_log
exp = explainer.explain_instance(X_validation[observation_1], predict_fn_log, num_features=100)
a=exp.as_list()
# print model_log.predict_proba(X_validation[observation_1]).astype(float)
# print exp.local_pred
for li in a:
    print li
# print X_train['453'].value_counts()

exp = explainer.explain_instance(X_validation[observation_1], predict_fn_rf, num_features=100)
a=exp.as_list()
# print model_log.predict_proba(X_validation[observation_1]).astype(float)
# print exp.local_pred
for li in a:
    print li
