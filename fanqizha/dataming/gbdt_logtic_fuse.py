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
from sklearn.preprocessing import OneHotEncoder
import sys
def trainmodel(dfname,modelname,weightjson):
    f_file = dconf.data_path + dfname
    outweight_file = dconf.config_path+weightjson
    model_file=dconf.model_path+modelname
    t_file=dconf.data_path+'11_15_test/'+'userfeaturecode_501.csv'
    out_file = dconf.data_path + 'score_gbdt_1' + dfname
    traindfall=pd.read_csv(f_file)
    traindfall['1'].replace([1, 0], [0, 1], inplace=True)
    testdf=pd.read_csv(t_file)
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
    print len(predictors)
    # print predictors
    # grd = GradientBoostingClassifier(learning_rate=0.1, n_estimators=70,max_depth=3, min_samples_leaf =70,
    #                                  min_samples_split =800, max_features=17, random_state=10)
    grd = GradientBoostingClassifier( n_estimators=50,max_depth=7,
                                     min_samples_split =800, max_features=17, random_state=10)
    grd_enc = OneHotEncoder()
    grd_lm = LogisticRegression(C=1.0,penalty='l1',solver='liblinear',multi_class='ovr')
    grd.fit(traindfall[predictors],traindfall[target])

    t=grd.apply(traindfall[predictors])[:, :, 0]
    tdf=pd.DataFrame(t)
    tdf.to_csv(dconf.config_path+'tdf.csv',index=False)
    tcol=tdf.columns.tolist()
    for i in range(len(tcol)):
        tcol[i]=str(tcol[i])+'n'
    tdf.columns=tcol
    tdf['0']=traindfall['0']
    talldf=traindfall.merge(tdf,on=['0','0'],how='left')


    te = grd.apply(testdf[predictors])[:, :, 0]
    tedf = pd.DataFrame(te)
    tcol = tedf.columns.tolist()
    for i in range(len(tcol)):
        tcol[i] = str(tcol[i]) + 'n'
    tedf.columns = tcol
    tedf['0'] = testdf['0']
    testalldf = testdf.merge(tedf, on=['0', '0'], how='left')

    predictors = [x for x in talldf.columns if x not in flist]
    print predictors

    # grd_enc.fit(grd.apply(traindfall[predictors])[:, :, 0])
    # a=grd_enc.transform(grd.apply(traindfall[predictors])[:, :, 0])
    # print a[0]
    # a=a[0].todense()
    # print type(a)
    # a=a.tolist()
    # print len(a[0])
    # print len(a)
    # grd_lm.fit(grd_enc.transform(grd.apply(traindfall[predictors])[:, :, 0]), traindfall[target])
    grd_lm.fit(talldf[predictors], traindfall[target])

    y_pred_grd_lm = grd_lm.predict_proba(testalldf[predictors])[:, 1]
    userid =testdf['0'].values.tolist()
    wfp = open(out_file, 'w')
    #
    for ui in range(0,len(userid)):
        wfp.write(str(userid[ui])+','+str(y_pred_grd_lm[ui])+'\n')

    # t=grd.apply(traindfall[predictors])[:, :, 0]
    # # print type(t)
    # # print grd.apply(traindfall[predictors])[:]
    # tdf=pd.DataFrame(t)
    # # print tdf.head()
    # tdf.to_csv(dconf.config_path+'tdf.csv',index=False)

    # logm = LogisticRegression(C=1.0,penalty='l1',solver='liblinear',multi_class='ovr')
    # logm.fit(traindfall[predictors],traindfall[target])
    #
    # wfp=open(outweight_file,'w')
    # weightlist=logm.coef_[0]
    # # # print weightlist
    # featureweidc={}
    # print nordic
    # for i in range(0,len(predictors)):
    #     print predictors[i],nordic[predictors[i]],weightlist[i]
    #     featureweidc[nordic[predictors[i]]]=round(weightlist[i],10)
    # featureweidc['bias']=round(logm.intercept_[0],10)
    # json.dump(featureweidc,wfp)
    # joblib.dump(logm, model_file)
    # feat_imp = pd.Series(logm.coef_[0], predictors).sort_values(ascending=False)
    # print feat_imp
    # feat_imp.plot(kind='bar', title='Feature Importances')
    # plt.ylabel('Feature Importance Score')
    # plt.show()


if __name__ == '__main__':
    trainmodel('codetraindfnorinner.csv','g1.model','w.json')