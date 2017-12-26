# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindatainner.csv'
fdf = pd.read_csv(f_file)
fdf['39u'] = 1.0 * fdf['39'] / (fdf['41'] + 1)
fdf = fdf.round(10)

s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
# s_file = '/Users/ufenqi/Downloads/featurename_update'
feaidlist = []
feanamelist = []
# feaidlist.append('0')
with open(s_file, 'r') as fp:
    for line in fp:
        linelist = line.strip().split(',')
        feaidlist.append(linelist[0])
        feanamelist.append(linelist[1])
columns = feaidlist
fdf=fdf[columns]
fdf.columns=feanamelist
desdf=fdf.describe().T
desdf['featurename']=desdf.index
col=desdf.columns.tolist()
desdf=desdf[col[-1:]+col[:-1]]
desdf.to_csv('/Users/ufenqi/Downloads/train.csv',index=False)