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
from sklearn.linear_model import LinearRegression
import offline_db_conf as dconf
import sys
from sklearn.svm import SVC
#lr是一个LogisticRegression模型
# joblib.dump(lr, 'lr.model')
# lr = joblib.load('lr.model')

def modelpre(dfname,modelname):
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
    target='1'
    IDcol='0'
    flist = []
    # flist = ['52', '43', '8', '11', '26', '62', '54']
    # flist = flist=['23','11','18t','24','25','45','43']
    # flist=['18t','27t','36t','37t','46t','20']
    flist.append(target)
    flist.append(IDcol)
    predictors = [x for x in testdfall.columns if x not in flist]
    predictors=noridlist
    # predictors.remove('0')
    print len(predictors)
    print '111',predictors
    logm=joblib.load(model_file)
    y_predprobl = logm.predict(testdfall[predictors])
    # y_predprobl = logm.predict_proba(testdfall[predictors])[:,1]

    wfp=open(out_file,'w')
    # # # print y_predprob
    userid =testdfall['0'].values.tolist()
    # usery=testdfall['1'].values.tolist()
    # # #

    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_predprobl[ui])+'\n')
    plt.figure(1)
    n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()

    # for i in range(0,10):
    #     num=0
    #     for ui in range(0,len(userid)):
    #         # index1 = int(0.1 * i * len(y_predprobl))
    #         if y_predprobl[ui]>0.1 * i:
    #             num+=1
    #     print 'scor:',i*0.1,'acc:',1.0*num/len(userid)
    # num = 0
    # for ui in range(0,len(userid)):
    #     # index1 = int(0.1 * i * len(y_predprobl))
    #     if y_predprobl[ui]>0.26:
    #         num+=1
    # print 'scor:',0.26,'acc:',1.0*num/len(userid)
def model_testpre(dfname,modelname):
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
    target='1'
    IDcol='0'
    flist = []
    # flist = ['14', '11', '44', '24', '5', '22']
    # flist = ['18t', '27t', '36t', '37t', '46t']
    flist.append(target)
    flist.append(IDcol)
    predictors = [x for x in testdfall.columns if x not in flist ]
    # predictors.remove('0')
    print len(predictors)
    print predictors
    logm=joblib.load(model_file)
    y_predl = logm.predict(testdfall[predictors])
    y_predprobl = logm.predict_proba(testdfall[predictors])[:,1]
    # print logm.predict_proba(testdfall[predictors])[:,1]
    print 'Logistic'
    print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_predl)
    print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprobl)

    wfp=open(out_file,'w')
    # # # print y_predprob
    userid =testdfall['0'].values.tolist()
    usery=testdfall['1'].values.tolist()
    # # #

    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_predprobl[ui])+'\n')
    # plt.figure(1)
    # n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
    #
    # for i in range(0,10):
    #     num=0
    #     for ui in range(0,len(userid)):
    #         # index1 = int(0.1 * i * len(y_predprobl))
    #         if y_predprobl[ui]>0.1 * i:
    #             num+=1
    #     print 'scor:',i*0.1,'acc:',1.0*num/len(userid)
    # for ui in range(0,len(userid)):
    #     # index1 = int(0.1 * i * len(y_predprobl))
    #     if y_predprobl[ui]>0.1 * 2.6:
    #         num+=1
    # print 'scor:',2.6,'acc:',1.0*num/len(userid)
if __name__ == '__main__':
    t=sys.argv[1]
    init_file=sys.argv[2]
    mode_file=sys.argv[3]
    if t=='traintest':
        model_testpre(init_file,mode_file)
    if t=='test':
        modelpre(init_file,mode_file)




