# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import numpy as np
import offline_db_conf as dconf
reload(sys)
tdf=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/traindata_use_bank_column.csv')
col1=tdf.columns.tolist()
lent = 0.2 * len(tdf)
for col in col1[1:]:
    tvc = tdf[col].value_counts()
    if tvc[tvc.index[0]] > lent:
        colind=col+'ind'
        tdf[colind]=tdf[col]
        tdf[colind]=tdf[colind].astype('string')
        tdf[colind][tdf[colind]=='-1.0']='no'
        tdf[colind][tdf[colind]!='no']='1'
        tdf[colind][tdf[colind]=='no']='0'
        tdf[colind]=tdf[colind].astype('int')
    tdf[col].replace(-1, tdf[col][tdf[col] != -1].quantile(0.5), inplace=True)
print tdf.describe()
tdf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/traindata_use_bank_miss.csv',index=False)