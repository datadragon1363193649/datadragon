# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
tdf=pd.read_excel('/Users/ufenqi/Downloads/lease_order.xlsx')
# print tdf[tdf[u'发货时间']!=pd.NaT].head()
# print tdf[tdf[u'发货时间'].dropna()]
index_list=tdf[u'发货时间'].dropna().index.tolist()
# times=tdf[u'发货时间'].dropna().tolist()
user_list=tdf['user_id'][tdf.index.isin(index_list)].tolist()
t1df=tdf[tdf['user_id'].isin(user_list)]
t1df.to_csv('/Users/ufenqi/Downloads/lease_order.csv',encoding='gbk',index=False)
# print tdf.head()