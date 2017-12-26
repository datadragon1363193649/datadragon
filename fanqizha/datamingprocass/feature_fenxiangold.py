# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

def percConvert(ser):
    return ser/float(ser[-1])
# 原始数据
f_file = '/Users/ufenqi/Downloads/traindfdelold.csv'
fenxiang1out_file='/Users/ufenqi/Downloads/fenxiang1old'
fdf = pd.read_csv(f_file)

col=['3_1','3']
fdf[col[0]],arrylist=pd.qcut(fdf[col[1]],15,labels=False,retbins=True,duplicates='drop')
fdf[col[0]].value_counts()
wfp = open(fenxiang1out_file, 'a')
strlist=[]
for i in range(0,len(arrylist)) :
    strlist.append(str(arrylist[i]))
wfp.write(col[0]+'\t'+'\t'.join(strlist)+'\n')
wfp.close()

a=pd.crosstab(fdf[col[0]],fdf['1'],margins=True).apply(percConvert,axis=1)
# # 百分比显示
a[0.0].sort_values()