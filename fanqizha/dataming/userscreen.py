# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
def screenrow():
    t1df=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/useridphn1.csv')
    col2=['65','66','67','68','69','70','71','72','73','74','75','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91','92','93','94','95','96','97','98','99','100','101','102','103']
    t2df=t1df[col2].T
    collist=[]
    del_useridlist=[]
    lent=0.8*len(col2)
    for col in t2df.columns:
        if len(t2df[t2df[col]==-1])>lent:
            del_useridlist+=t1df['0'][t1df.index==col].values.tolist()
            collist.append(col)
    # print len(collist),len()
    deldf=t1df[t1df['0'].isin(del_useridlist)]
    print len(collist)
    # print deldf.describe()
    deldf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/useridphn1_bank_del.csv',index=False)
    t2df=t1df.drop(collist,axis=0)
    t2df.to_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/useridphn1_bank_use.csv',index=False)
def screencolumn():
    t1df=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/traindata_use_bank.csv')
    col1=t1df.columns.tolist()
    lent = 0.5* len(t1df)
    print len(t1df.columns)
    for col in col1[1:]:
        tvc = t1df[col].value_counts()
        if len(t1df[col].unique()) < 2:
            del t1df[col]
            continue
        if tvc[tvc.index[0]] > lent:
            del t1df[col]
            continue
            t1df[col] = t1df[col].astype('string')
            t1df[col][t1df[col] == str(tvc.index[0])] = 'no'
            t1df[col][t1df[col] != 'no'] = '1'
            t1df[col][t1df[col] == 'no'] = '0'
            t1df[col] = t1df[col].astype('float64')
    print len(t1df.columns)
    # t1df.to_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/traindata_use_bank_column.csv',index=False)
if __name__ == '__main__':
    screencolumn()
