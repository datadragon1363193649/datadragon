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
import sys
import offline_db_conf as dconf
def trainmodel(dfname,testname):
    f_file = dconf.data_path + dfname
    test_file=dconf.data_path + testname
    yu_file=dconf.data_path+'score_710'
    yudic={}
    with open(yu_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            # print linelist
            yudic[int(linelist[0])]=int(linelist[-1])
    # outweight_file = dconf.config_path+weightjson
    # model_file=dconf.model_path+modelname
    traindfall=pd.read_csv(f_file)
    testdfall=pd.read_csv(test_file)

    # traindfall['1'].replace([1, 0], [0, 1], inplace=True)
    print traindfall['1'].value_counts()
    # s_file = dconf.config_path + 'featurename_online'
    # s_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/callfeaturename'
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
    # flist = ['52', '43', '8', '11', '26','62','54']
    # flist=['23','11','18t','24','25','45','43']
    # flist=['18t','27t','36t','37t','46t','20']
    flist.append(target)
    flist.append(IDcol)
    predictors = [x for x in traindfall.columns if x not in flist ]
    predictors=noridlist
    # print len(predictors)
    # print predictors

    # logm = LogisticRegression(C=1.0,penalty='l1',solver='liblinear',multi_class='ovr')
    # logm.fit(traindfall[predictors],traindfall[target])
    # gbm1 = GradientBoostingClassifier()
    # gbm1.fit(traindfall[predictors],traindfall[target])
    # y_pred = gbm1.predict(testdfall[predictors])
    # y_predprob = gbm1.predict_proba(testdfall[predictors])[:,1]
    #
    rf = RandomForestClassifier()
    rf.fit(traindfall[predictors],traindfall[target])
    y_predprob = rf.predict_proba(testdfall[predictors])[:, 1]
    targetlist=[]
    labellist=testdfall['0'].values.tolist()
    for la in labellist:
        targetlist.append(yudic[la])
    # aucdf=pd.DataFrame(targetlist,y_predprob,columns=['yuqiday','sfscore2' ])
    aucdf = pd.DataFrame({'yuqiday':targetlist, 'sfscore2':y_predprob})
    print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['sfscore2'] = aucdf['sfscore2'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']
    aucdf = aucdf[aucdf['yuqiday1'] > -1]
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    print "sfscore2 AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['sfscore2'])
    plt.figure(1)
    n, bins, patches = plt.hist(y_predprob, 100, normed=1, facecolor='g', alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()
    y_predprobl = aucdf['sfscore2'].values.tolist()
    plt.figure(1)
    n, bins, patches = plt.hist(y_predprobl, 100, normed=1, facecolor='g', alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()
    plt.figure(1)
    plt.plot([0, 1], [0, 1], 'k--')


if __name__ == '__main__':
    # input_file = sys.argv[1]
    # model_name=sys.argv[2]
    # weightjson=sys.argv[3]
    input_file='codenordeltraindata_addfu.csv'
    testname='codenordelscore_710_addfu.csv'
    trainmodel(input_file,testname)

