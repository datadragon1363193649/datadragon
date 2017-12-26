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
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import seaborn as sns
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import roc_curve
from sklearn.externals import joblib
model1_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/model/userfencall'
model2_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/model/userfenbank'
wecash_file='/Users/ufenqi/Downloads/testuserallgbdt'
calldic={}
with open(model1_file,'r')as fp:
    for line in fp :
        linelist=line.strip().split(',')
        calldic[linelist[0]]=float(linelist[2])

wecashdic={}
with open(wecash_file,'r')as fp:
    for line in fp :
        linelist=line.strip().split(',')
        wecashdic[linelist[0]]=float(linelist[3])
userydic={}
bankcall={}
with open(model2_file,'r')as fp:
    for line in fp:
        linelist = line.strip().split(',')
        bankcall[linelist[0]]=float(linelist[2])
        userydic[linelist[0]]=float(linelist[1])
weightlist=range(1,10)
for wei in weightlist:
    uy=[]
    ufen=[]
    welist=[]
    for uid in calldic:
        uy.append(userydic[uid])
        fen=calldic[uid]*wei*0.1+bankcall[uid]*(1-wei*0.1)
        ufen.append(fen)
        welist.append(wecashdic[uid])
    fpr_grd_lm, tpr_grd_lm, _ = roc_curve(uy, ufen)
    fpr_grd_lmw, tpr_grd_lmw, _ = roc_curve(uy, welist, pos_label=0)
    print 'call权重',wei
    print "AUC Score (Train): %f" % metrics.roc_auc_score(uy, ufen)
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')
    # # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
    # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT')
    # plt.plot(fpr_grd_lmw, tpr_grd_lmw, label='wecash')
    # plt.xlabel('False positive rate')
    # plt.ylabel('True positive rate')
    # plt.legend(loc='best')
    # plt.show()
    # plt.figure(1)
    # n, bins, patches = plt.hist(welist, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()

    plt.figure(2)
    n,bins,patches=plt.hist(ufen,100,normed=1,facecolor='g',alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()