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
def trainmodel(dfname,modelname,weightjson):
    UID = dconf.uid
    TARGET = dconf.target
    f_file = dconf.data_path + dfname
    outweight_file = dconf.config_path+weightjson
    model_file=dconf.model_path+modelname
    traindfall=pd.read_csv(f_file)
    # traindfall['1'].replace([1, 0], [0, 1], inplace=True)
    print traindfall[TARGET].value_counts()
    name_file=dconf.config_path + dconf.featurename
    noridlist=[]
    nordic={}
    with open(name_file, 'r') as fp:
        for line in fp:
            norfealist = line.strip().split(',')
            noridlist.append(norfealist[0])
            nordic[norfealist[0]] = norfealist[1]
    target=TARGET
    IDcol=UID
    flist=[]
    flist.append(target)
    flist.append(IDcol)
    predictors=noridlist

    logm = LogisticRegression(C=1.0,penalty='l1',solver='liblinear',multi_class='ovr')
    logm.fit(traindfall[predictors],traindfall[target])

    wfp=open(outweight_file,'w')
    weightlist=logm.coef_[0]
    featureweidc={}
    for i in range(0,len(predictors)):
        print predictors[i],nordic[predictors[i]],weightlist[i]
        featureweidc[nordic[predictors[i]]]=round(weightlist[i],10)
    featureweidc['bias']=round(logm.intercept_[0],10)
    json.dump(featureweidc,wfp)
    joblib.dump(logm, model_file)
    feat_imp = pd.Series(logm.coef_[0], predictors).sort_values(ascending=False)
    # 特征重要型绘图
    feat_imp.plot(kind='bar', title='Feature Importances')
    plt.ylabel('Feature Importance Score')
    plt.show()


if __name__ == '__main__':
    input_file = sys.argv[1]
    model_name=sys.argv[2]
    weightjson=sys.argv[3]
    trainmodel(input_file,model_name,weightjson)
