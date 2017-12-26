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
testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/ttestdata.csv')
traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/ttraindata.csv')
# 原始数据
f_file = '/Users/ufenqi/Downloads/fanqizha/30trainuserid'
# print testdf['userid']
ff =[]
with open(f_file, 'r') as fp:
    for line in fp:
        linelist = line.strip().split('\t')
        ff.append(float(linelist[0]))
testdf1=testdf[testdf['userid'].isin(ff)]
traindf=traindf[traindf['userid'].isin(ff)]
testdf1.to_csv('/Users/ufenqi/Downloads/fanqizha/ttestdata30.csv',index=False)
traindf.to_csv('/Users/ufenqi/Downloads/fanqizha/ttraindata30.csv',index=False)
# print testdf.__len__()
# print testdf1.__len__()