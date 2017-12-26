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
from sklearn.metrics import roc_curve
from sklearn.metrics import auc
import pandas as pd
int_file = '/Users/ufenqi/Documents/dataming/base1/data/view_sample_selection'
userlist = []
with open(int_file, 'r') as fp2:
    for line in fp2:
        linelist = line.strip().split(',')
        userlist.append(linelist)
        # print len(linelist)
aucdf = pd.DataFrame(userlist, columns=['userid', 'passtf', 'myself'])
# print aucdf.head()
aucdf['passtf'] = aucdf['passtf'].astype('float')
aucdf['myself'] = aucdf['myself'].astype('float')
# aucdf['wecash'] = aucdf['wecash'].astype('float')
# aucdf['gbdt'] = aucdf['gbdt'].astype('float')
aucdf['yuqiday1'] = aucdf['passtf']
# aucdf=aucdf[aucdf['yuqiday1']>-1]
print aucdf.__len__()
# aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
# aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['myself'], pos_label=1)
print 'myself', auc(fpr, tpr)