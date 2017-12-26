# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

# 原始数据
f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindataall.csv'
test_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdataall.csv'
fdf = pd.read_csv(f_file)
testdf = pd.read_csv(test_file)
columns=fdf.columns.tolist()
columns.remove('0')
columns.remove('1')
columns.remove('2')
# columns.remove('19')
# columns.remove('43')
# columns.remove('45')
# columns.remove('54')
# columns.remove('55')
columns.remove('63')
columns.remove('69')
stain=columns.index('67')
for col in range(stain,len(columns)):
    print fdf[columns[col]].value_counts()