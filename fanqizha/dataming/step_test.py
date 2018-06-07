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
    noridlist=['1']
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

    data_for_select=traindfall[predictors]
    # lg_m1=forward_select


if __name__ == '__main__':
    # input_file = sys.argv[1]
    # model_name=sys.argv[2]
    # weightjson=sys.argv[3]
    input_file='train_call_tr_0.csv'
    model_name='step_model'
    weightjson='step_json'
    trainmodel(input_file,model_name,weightjson)