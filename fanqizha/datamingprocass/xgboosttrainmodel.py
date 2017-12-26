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
import xgboost as xgb
traindfall=pd.read_csv('trainnorandcode.csv')
testdfall=pd.read_csv('testnorandcode.csv')
# testdfall=pd.read_csv('testnorandcoderealrenzheng.csv')
# use_file='callfeaturename'
# use_file='bankfeaturename
# linelist=[]
# with open(use_file, 'r') as fp:
#     for line in fp:
#         linelist = line.strip().split(',')
# linelist1=[]
# for lin in linelist:
#     if 'fen' not in lin:
#         linelist1.append(lin)
target='1'
IDcol='0'
predictors = [x for x in traindfall.columns if x not in [target, IDcol] ]
# predictors=linelist1
# print predictors
# print testdfall.columns
# for pre in predictors:
#     if pre in testdfall.columns:
#         continue
#     print pre
# predictors.remove('63')
params = {
    'min_child_weight': 100,
    'eta': 0.02,
    'colsample_bytree': 0.7,
    'max_depth': 12,
    'subsample': 0.7,
    'alpha': 1,
    'gamma': 1,
    'silent': 1,
    'verbose_eval': True,
    'seed': 12
}
rounds=10
xgtrain = xgb.DMatrix(traindfall[predictors], label=traindfall[target])
bst = xgb.train(params, xgtrain, num_boost_round=rounds)
dtrain_predictions = bst.predict(testdfall[predictors])
dtrain_predprob = bst.predict_proba(testdfall[predictors])[:, 1]

# Print model report:
print "Model Report"
print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], dtrain_predictions)
print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], dtrain_predprob)