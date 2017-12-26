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

s_file='/Users/ufenqi/Downloads/feayuqiself601'
welist=[]
daylist=[]
selflist=[]
with open(s_file,'r') as fp:
    for line in fp:
        linelist=line.strip().split(',')
        dayi=int(linelist[3])
        g=int(linelist[2])
        print g
        if g>0:
            welist.append(float(linelist[1]))
            selflist.append(float(linelist[3]))
            daylist.append(0)
        elif dayi>9:
            daylist.append(0)
            welist.append(float(linelist[1]))
            selflist.append(float(linelist[4]))
        else:
            daylist.append(1)
            welist.append(float(linelist[1]))
            selflist.append(float(linelist[4]))
print "AUC Score (Train): %f" % metrics.roc_auc_score(daylist, welist)
print "self AUC Score (Train): %f" % metrics.roc_auc_score(daylist, selflist)
fpr_grd_lm, tpr_grd_lm, _ = roc_curve(daylist, selflist)
fpr_grd_lmw, tpr_grd_lmw, _ = roc_curve(daylist, welist)
plt.figure(1)
plt.plot([0, 1], [0, 1], 'k--')
# plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
plt.plot(fpr_grd_lm, tpr_grd_lm, label='self')
plt.plot(fpr_grd_lmw, tpr_grd_lmw, label='wecash')
plt.xlabel('False positive rate')
plt.ylabel('True positive rate')
plt.legend(loc='best')
plt.show()
