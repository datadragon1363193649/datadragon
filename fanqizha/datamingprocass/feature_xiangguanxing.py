# -*- cocoding: utf-8 -*-
from sklearn.pipeline import Pipeline
# from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
import pandas as pd
from sklearn import cross_validation, metrics
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import seaborn as sns
def xiangguan2():
    traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodeandnum.csv')
    target='1'
    IDcol='0'
    predictors = [x for x in traindf.columns if x not in [target, IDcol] ]
    qupredic=traindf.columns.tolist()
    traindf1=traindf[predictors]
    print len(qupredic)
    for pre in predictors:
        # print pre
        # print  traindf.corrwith(traindf[pre]).sort_values(ascending=False).head()
        precorr=traindf1.corrwith(traindf[pre])
        prelist=precorr[precorr>0.9].index.tolist()
        for pl in prelist[1:]:
            if pl in qupredic:
                qupredic.remove(pl)
                predictors.remove(pl)
    print len(qupredic)
    print qupredic
    traindf3=traindf[qupredic]
    prenan=traindf3.corrwith(traindf3['1']).sort_values(ascending=False)
    # print prenan
    prenan=prenan.fillna(200)
    # print prenan
    prelist=prenan[prenan>100].index.tolist()
    # print prelist
    for pl in prelist:
        if pl in qupredic:
            qupredic.remove(pl)
            # predictors.remove(pl)
    traindf4=traindf3[qupredic]
    print len(qupredic)
    traindf4.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodeandnumqu.csv',index=False)
def xiangguantarget():
    traindf = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
    target = '1'
    IDcol = '0'
    predictors = [x for x in traindf.columns if x not in [target, IDcol]]
    qupredic = traindf.columns.tolist()
    print len(qupredic)
    traindf1 = traindf[predictors]

    prenan = traindf.corrwith(traindf['1']).sort_values(ascending=False)
    print prenan
    # print prenan
    # print prenan
    prelist = prenan[(prenan < 0.05)&(prenan>-0.05)].index.tolist()
    print prelist
    for pl in prelist:
        if pl in qupredic:
            qupredic.remove(pl)
            # predictors.remove(pl)
    traindf4 = traindf[qupredic]
    print len(qupredic)
    traindf4.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodequtaget.csv', index=False)
if __name__ == '__main__':
    xiangguantarget()
