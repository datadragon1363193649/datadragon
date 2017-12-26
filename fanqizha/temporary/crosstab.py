# -*- encoding: utf-8 -*-
import pandas as pd
import matplotlib.pylab as plt
tdf=pd.read_csv('/Users/ufenqi/Documents/dataming/model_yu/data/codenordelindtrain_phn_merge_1.csv')
columnlist=tdf.columns.tolist()
def percConvert(ser):return ser/float(ser[-1])
for col in columnlist[2:]:
    temp1 = pd.crosstab(tdf[col], tdf['1'])
    temp1[[0.0, 1.0]].plot(kind='bar', stacked=True, color=['red', 'blue'], grid=False)
    temp = pd.crosstab(tdf[col], tdf['1'], margins=True).apply(percConvert, axis=1)
    temp[[0.0, 1.0]].plot(kind='bar', stacked=True, color=['red', 'blue'], grid=False)
    plt.show()

# temp1 = pd.crosstab([tdf['4'],tdf['5']], tdf['1'])
# temp1[[0.0, 1.0]].plot(kind='bar', stacked=True, color=['red', 'blue'], grid=False)
# temp = pd.crosstab([tdf['4'],tdf['5']], tdf['1'], margins=True).apply(percConvert, axis=1)
# temp[[0.0, 1.0]].plot(kind='bar', stacked=True, color=['red', 'blue'], grid=False)
# plt.show()