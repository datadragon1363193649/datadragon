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
# 添加目标值
def target():
    s1file='/Users/ufenqi/Documents/dataming/base1/data/phnalltarget3'
    s2file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeature_4'
    outfile='/Users/ufenqi/Documents/dataming/base1/data/userfeatureall3_4'
    s1dic={}
    wfp=open(outfile,'w')
    with open(s1file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            s1dic[linelist[0]]=linelist[1]
    with open(s2file,'r') as fp:
        for line in fp:
            linelist=line.strip().split('\t')
            if linelist[0] in s1dic:
                lineall=[linelist[0],s1dic[linelist[0]]]+linelist[1:]
                wfp.write('\t'.join(lineall)+'\n')
            else:
                print linelist
# 规范数据集
def train():
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindatainner.csv'
    fdf = pd.read_csv(f_file)
    fdf['13']=fdf['13']/60
    fdf['29'] = fdf['29'] / 60
    fdf['32'] = fdf['32'] / 60
    fdf['35'] = fdf['35'] / 60
    fdf['39u'] = 1.0 * fdf['39'] / (fdf['41'] + 1)
    s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
    # s_file = '/Users/ufenqi/Downloads/featurename_update'
    feaidlist = []
    feanamelist = []
    feaidlist.append('0')
    feaidlist.append('1')
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
    columns = feaidlist
    fdf=fdf[columns]
    fdf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/traindatainner.csv',index=False)
if __name__ == '__main__':
    train()