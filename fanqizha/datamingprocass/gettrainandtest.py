# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfdel.csv')
featrain=traindf.sample(frac=0.75,random_state=22)
featest=traindf.drop(featrain.index)
featrain.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/ttrain.csv',index=False)
featest.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/ttest.csv',index=False)
