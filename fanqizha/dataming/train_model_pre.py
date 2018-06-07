# -*- cocoding: utf-8 -*-
from sklearn.pipeline import Pipeline
# from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import (RandomTreesEmbedding, RandomForestClassifier,
                              GradientBoostingClassifier)
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
import pandas as pd
from sklearn import cross_validation, metrics
import json
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import seaborn as sns
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import roc_curve
from sklearn.externals import joblib
import offline_db_conf as dconf
import sys
#lr是一个LogisticRegression模型
# joblib.dump(lr, 'lr.model')
# lr = joblib.load('lr.model')


def model_target_pre(dfname,modelname):
    UID = dconf.uid
    TARGET = dconf.target
    f_file = dconf.data_path + dfname
    testdfall=pd.read_csv(f_file)
    name_file = dconf.config_path + dconf.featurename
    model_file = dconf.model_path + modelname
    out_file = dconf.data_path +'score'+dfname
    noridlist = []
    nordic = {}
    with open(name_file, 'r') as fp:
        for line in fp:
            norfealist = line.strip().split(',')
            noridlist.append(norfealist[0])
            nordic[norfealist[0]] = norfealist[1]
    target=TARGET
    IDcol=UID
    flist = []
    flist.append(target)
    flist.append(IDcol)
    predictors = [x for x in testdfall.columns if x not in flist ]
    print len(predictors)
    print predictors
    logm=joblib.load(model_file)
    y_predl = logm.predict(testdfall[predictors])
    y_predprobl = logm.predict_proba(testdfall[predictors])[:,1]
    print 'Logistic'
    print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_predl)
    print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprobl)

    wfp=open(out_file,'w')
    userid =testdfall[UID].values.tolist()
    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_predprobl[ui])+'\n')
def modelpre(dfname,modelname):
    UID = dconf.uid
    TARGET = dconf.target
    f_file = dconf.data_path + dfname
    testdfall=pd.read_csv(f_file)
    name_file = dconf.config_path + dconf.featurename
    model_file = dconf.model_path + modelname
    out_file = dconf.data_path +'score'+dfname
    noridlist = []
    nordic = {}
    with open(name_file, 'r') as fp:
        for line in fp:
            norfealist = line.strip().split(',')
            noridlist.append(norfealist[0])
            nordic[norfealist[0]] = norfealist[1]
    predictors=noridlist
    print len(predictors)
    print '111',predictors
    logm=joblib.load(model_file)
    y_predprobl = logm.predict_proba(testdfall[predictors])[:,1]
    print logm.predict_proba(testdfall[predictors])
    wfp=open(out_file,'w')
    userid =testdfall[UID].values.tolist()

    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_predprobl[ui]+0.49)+'\n')
    # 预测值分布图
    plt.figure(1)
    n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    plt.grid(True)
    plt.show()

    for i in range(0,10):
        num=0
        for ui in range(0,len(userid)):
            # index1 = int(0.1 * i * len(y_predprobl))
            if y_predprobl[ui]>0.1 * i:
                num+=1
        print 'scor:',i*0.1,'acc:',1.0*num/len(userid)
    num = 0
    for ui in range(0,len(userid)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if y_predprobl[ui]>0.26:
            num+=1
    print 'scor:',0.26,'acc:',1.0*num/len(userid)
if __name__ == '__main__':
    t=sys.argv[1]
    init_file=sys.argv[2]
    mode_file=sys.argv[3]
    if t=='targettest':
        model_target_pre(init_file,mode_file)
    if t=='test':
        modelpre(init_file,mode_file)




