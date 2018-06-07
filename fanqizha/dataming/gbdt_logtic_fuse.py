#! /usr/bin/env python
# -*- encoding: utf-8 -*-
import numpy as np
import offline_db_conf as dconf
np.random.seed(10)

import matplotlib.pyplot as plt

from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (RandomTreesEmbedding, RandomForestClassifier,
                              GradientBoostingClassifier)
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn import cross_validation, metrics
from sklearn.pipeline import make_pipeline
from sklearn.metrics import roc_curve
from sklearn.metrics import auc
from scipy.stats import ks_2samp
from sklearn.metrics import r2_score
import sys
from sklearn.neural_network import MLPClassifier
import pandas as pd
# 特征融合
def train(dfname,modelname,weightjson,testname):
    f_file = dconf.data_path + dfname
    t_file=dconf.data_path + testname
    outweight_file = dconf.config_path+weightjson
    model_file=dconf.model_path+modelname
    out_file = dconf.data_path + 'score_gbdt' + testname
    traindfall=pd.read_csv(f_file)
    testdf=pd.read_csv(t_file)
    print traindfall['1'].value_counts()
    name_file=dconf.config_path + dconf.featurename
    noridlist=[]
    nordic={}
    with open(name_file, 'r') as fp:
        for line in fp:
            norfealist = line.strip().split(',')
            noridlist.append(norfealist[0])
            nordic[norfealist[0]] = norfealist[1]
    target='1'
    IDcol='0'
    flist=[]
    flist.append(target)
    flist.append(IDcol)
    predictors=noridlist
    n_estimator = 10
    X_train=traindfall[predictors]
    y_train=traindfall[target]
    X_test=testdf[predictors]
    grd = GradientBoostingClassifier(n_estimators=n_estimator)
    grd_enc = OneHotEncoder()
    grd_lm = LogisticRegression()
    grd.fit(X_train, y_train)
    grd_enc.fit(grd.apply(X_train)[:, :, 0])
    x=grd_enc.transform(grd.apply(X_train)[:, :, 0])
    print type(x)
    xlist= x.toarray().tolist()
    xindex=traindfall['0'].values.tolist()
    for i in range(len(xindex)):
        xlist[i].append(xindex[i])
    xdf=pd.DataFrame(xlist)

    columnlist=xdf.columns.tolist()
    for ci in range(len(columnlist)):
        columnlist[ci]=str(columnlist[ci])+"g"
    columnlist[-1]='0'
    print columnlist
    xdf.columns=columnlist
    print xdf.head()
    traindf=traindfall.merge(xdf,on=['0','0'],how='left')
    print traindf.head()
    predictors+=columnlist[:-1]
    grd_lm.fit(traindf[predictors], y_train)

    # 测试集
    testx=grd_enc.transform(grd.apply(X_test)[:, :, 0])
    testxlist = testx.toarray().tolist()
    testindex = testdf['0'].values.tolist()
    for i in range(len(testindex)):
        testxlist[i].append(testindex[i])
    testgdf = pd.DataFrame(testxlist)
    testgdf.columns = columnlist
    print testgdf.head()
    testdatadf = testdf.merge(testgdf, on=['0', '0'], how='left')
    #
    y_pred_grd_lm = grd_lm.predict_proba(testdatadf[predictors])[:, 1]
    wfp = open(out_file, 'w')
    userid = testdf['0'].values.tolist()
    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_pred_grd_lm[ui])+'\n')
# 单用GBDT
def gbdt_train(dfname,modelname,weightjson,testname):
    f_file = dconf.data_path + dfname
    t_file=dconf.data_path + testname
    outweight_file = dconf.config_path+weightjson
    model_file=dconf.model_path+modelname
    out_file = dconf.data_path + 'gbdt' + testname
    traindfall=pd.read_csv(f_file)
    testdf=pd.read_csv(t_file)
    print traindfall['1'].value_counts()
    name_file=dconf.config_path + dconf.featurename
    noridlist=[]
    nordic={}
    with open(name_file, 'r') as fp:
        for line in fp:
            norfealist = line.strip().split(',')
            noridlist.append(norfealist[0])
            nordic[norfealist[0]] = norfealist[1]
    target='1'
    IDcol='0'
    flist=[]
    flist.append(target)
    flist.append(IDcol)
    predictors = [x for x in traindfall.columns if x not in flist ]
    predictors=noridlist
    n_estimator = 10
    grd = GradientBoostingClassifier(n_estimators=n_estimator)
    grd.fit(traindfall[predictors], traindfall['1'])
    y_predprob = grd.predict_proba(testdf[predictors])[:, 1]
    wfp = open(out_file, 'w')
    userid = testdf['0'].values.tolist()
    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_predprob[ui])+'\n')
def neural_network(dfname,modelname,weightjson,testname):
    f_file = dconf.data_path + dfname
    t_file=dconf.data_path + testname
    outweight_file = dconf.config_path+weightjson
    model_file=dconf.model_path+modelname
    out_file = dconf.data_path + 'score_network' + testname
    traindfall=pd.read_csv(f_file)
    testdf=pd.read_csv(t_file)
    print traindfall['1'].value_counts()
    name_file=dconf.config_path + dconf.featurename
    noridlist=[]
    nordic={}
    with open(name_file, 'r') as fp:
        for line in fp:
            norfealist = line.strip().split(',')
            noridlist.append(norfealist[0])
            nordic[norfealist[0]] = norfealist[1]
    target='1'
    IDcol='0'
    flist=[]
    flist.append(target)
    flist.append(IDcol)
    predictors=noridlist
    n_estimator = 10
    X_train=traindfall[predictors]
    y_train=traindfall[target]
    X_test=testdf[predictors]
    grd = GradientBoostingClassifier(n_estimators=n_estimator)
    grd_enc = OneHotEncoder()
    grd_lm = LogisticRegression()
    grd.fit(X_train, y_train)
    grd_enc.fit(grd.apply(X_train)[:, :, 0])
    x=grd_enc.transform(grd.apply(X_train)[:, :, 0])
    print type(x)
    xlist= x.toarray().tolist()
    xindex=traindfall['0'].values.tolist()
    for i in range(len(xindex)):
        xlist[i].append(xindex[i])
    xdf=pd.DataFrame(xlist)

    columnlist=xdf.columns.tolist()
    for ci in range(len(columnlist)):
        columnlist[ci]=str(columnlist[ci])+"g"
    columnlist[-1]='0'
    print columnlist
    xdf.columns=columnlist
    print xdf.head()
    traindf=traindfall.merge(xdf,on=['0','0'],how='left')
    print traindf.head()
    predictors+=columnlist[:-1]
    # grd_lm.fit(traindf[predictors], y_train)
    clf = MLPClassifier(solver='lbfgs', alpha=1e-5,
                        hidden_layer_sizes=(5, 2), random_state=1)

    clf.fit(traindf[predictors], y_train)
    # print 'predict\t', clf.predict_proba([[2., 2.], [1., 2.]])
    # 测试集
    testx=grd_enc.transform(grd.apply(X_test)[:, :, 0])
    testxlist = testx.toarray().tolist()
    testindex = testdf['0'].values.tolist()
    for i in range(len(testindex)):
        testxlist[i].append(testindex[i])
    testgdf = pd.DataFrame(testxlist)
    testgdf.columns = columnlist
    print testgdf.head()
    testdatadf = testdf.merge(testgdf, on=['0', '0'], how='left')
    # print 'predict\t', clf.predict_proba(testdatadf[predictors])
    y_pred_grd_lm = clf.predict_proba(testdatadf[predictors])[:, 1]
    wfp = open(out_file, 'w')
    userid = testdf['0'].values.tolist()
    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_pred_grd_lm[ui])+'\n')
if __name__ == '__main__':
    # input_file = sys.argv[1]
    # model_name = sys.argv[2]
    # weightjson = sys.argv[3]
    input_file='train_call_tr_0.csv'
    model_name='g_model'
    weightjson='g_weight.json'
    testname='codenordelclasstest_data_90101.csv'
    # train(input_file, model_name, weightjson,testname)
    # gbdt_train(input_file, model_name, weightjson, testname)
    neural_network(input_file, model_name, weightjson,testname)
