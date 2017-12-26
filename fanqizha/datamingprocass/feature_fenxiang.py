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
f_file = '/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/traindfnor.csv'
fenxiang1out_file='/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/fenxiang1'
numbercnt_file='/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/numbercnt'
wfp = open(fenxiang1out_file, 'w')
nwfp = open(numbercnt_file, 'w')
fdf = pd.read_csv(f_file)
columnslist=fdf.columns.tolist()
columnslist.remove('0')
columnslist.remove('1')
# columnslist.remove('2')
columnslist.remove('63')
numcol=[]
nonumcol=[]
nonumcol.append('0')
nonumcol.append('1')
# nonumcol.append('2')
nonumcol.append('63')
dflen=fdf.__len__()
for col in columnslist:
    fdf[col],arrylist=pd.qcut(fdf[col],10,labels=False,retbins=True,duplicates='drop')
    colmaxlen = fdf[col].value_counts().values.tolist()[0]
    if colmaxlen > (dflen * 0.8):
        del fdf[col]
        continue
    strlist=[]
    for i in range(0,len(arrylist)) :
        strlist.append(str(arrylist[i]))
    if len(arrylist)==11:
        numcol.append(col)
    else:
        nonumcol.append(col)
    wfp.write(col+'\t'+'\t'.join(strlist)+'\n')
nwfp.write(str(len(nonumcol)))
nwfp.close()
nonumcol+=numcol
ndf=fdf[nonumcol]
ndf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/traindata_code.csv',index=False)
wfp.close()
# a=pd.crosstab(fdf[col[0]],fdf['1'],margins=True).apply(percConvert,axis=1)
# # # 百分比显示
# a[0.0].sort_values()