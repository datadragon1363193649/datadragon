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
#lr是一个LogisticRegression模型
# joblib.dump(lr, 'lr.model')
# lr = joblib.load('lr.model')


testdfall=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeaturecode_501.csv')

use_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/callfeaturename'
# use_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/bankfeaturename'
linelist=[]
with open(use_file, 'r') as fp:
    for line in fp:
        linelist = line.strip().split(',')
linelist.remove('27lognorfen10')
linelist.remove('18lognorfen10')
print len(linelist)
linelist1=[]
for lin in linelist:
    if 'fen' not in lin:
        linelist1.append(lin)
# print linelist
# sns.boxplot(traindf['73lognor'])
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainfenxiang.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testfenxiang.csv')
target='1'
IDcol='0'
# print testdf[target].value_counts()
# predictors = [x for x in traindfall.columns if x not in [target, IDcol] ]
# for pre in predictors:
#     if '63'in pre:
predictors=linelist

print len(predictors)
print predictors
logm=joblib.load( '/Users/ufenqi/Documents/dataming/base1/model/lr30.model')
# print logm.coef_,logm.intercept_
# print type(logm.coef_),type(logm.intercept_)
# for i in predictors:
    # print testdfall[i]
y_predl = logm.predict(testdfall[predictors])
y_predprobl = logm.predict_proba(testdfall[predictors])[:,1]
out_file='/Users/ufenqi/Documents/dataming/base1/data/userfeaturecfen501_30'
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

for i in range(0,10):
    num=0
    for ui in range(0,len(userid)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if y_predprobl[ui]>0.1 * i:
            num+=1
    print 'scor:',i*0.1,'acc:',1.0*num/len(userid)
for ui in range(0,len(userid)):
    # index1 = int(0.1 * i * len(y_predprobl))
    if y_predprobl[ui]>0.1 * 2.6:
        num+=1
print 'scor:',0.26,'acc:',1.0*num/len(userid)



