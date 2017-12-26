# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
from sklearn import preprocessing
import numpy as np

f_file = '/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/train_lbl_binmax'
ff = []
with open(f_file, 'r') as fp:
    for line in fp:
        line1 = []
        linelist = line.strip().split('\t')
        ff.append(linelist)
fdf = pd.DataFrame(ff)
fdf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainbinmax.csv',index=False)


f_file = '/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/test_lbl_binmax'
ff = []
with open(f_file, 'r') as fp:
    for line in fp:
        line1 = []
        linelist = line.strip().split('\t')
        ff.append(linelist)
fdf = pd.DataFrame(ff)
fdf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testbinmax.csv',index=False)

tdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainbinmax.csv')
tedf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testbinmax.csv')
tcol=tdf.columns.tolist()
nonuml=[]
numl=[]
nonuml.append('0')
tcol.remove('0')
# dflen=tdf.__len__()
dflen=tdf.__len__()
for col in tcol:
    colmaxlen = tdf[col].value_counts().values.tolist()[0]
    if colmaxlen > (dflen * 0.8):
        continue
    if len(tdf[col].value_counts())<7:
        nonuml.append(col)
    else:numl.append(col)
print nonuml.__len__()
nonuml+=numl
traindf=tdf[nonuml]
testdf=tedf[nonuml]
traindf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainfenxiangmax.csv',index=False)
testdf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testfenxiangmax.csv',index=False)