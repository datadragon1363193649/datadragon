# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
from sklearn import preprocessing
import numpy as np
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

def trainonehot():
    lb = preprocessing.LabelBinarizer()
    # 原始数据
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnor.csv'
    fdf = pd.read_csv(f_file)
    colnew=[]
    colnew.append('2')
    colnew.append('63')

    for col in colnew:
        fdf[col+'one'] = lb.fit_transform(fdf[col])
    print fdf.head()
def testonehot():
    lb = preprocessing.LabelBinarizer()
    x_file = '/Users/ufenqi/Downloads/traindata_codeinner_10.csv'
    f_file = '/Users/ufenqi/Downloads/testfenxiang_code.csv'
    xfdf = pd.read_csv(x_file)
    fdf = pd.read_csv(f_file)
    lb.fit_transform(xfdf['2'])
    final_output = lb.transform(fdf['2'])
    lb.fit_transform(xfdf['63'])
    final_output= np.hstack((final_output,lb.transform(fdf['63'])))
    for i in range(0,len(final_output)):
        outlist=[]
        for j in final_output[i]:
            outlist.append(str(j))
if __name__ == '__main__':
    trainonehot()
# wfp.close()
# np.s


# fnew['is0']=fnew.index
# fnew['is1']=fnew.index
# fnew['is2']=fnew.index
# fnew['is3']=fnew.index
# fnew['is4']=fnew.index
# print fnew.head()
# fnewt=fdf[colnew].T
# print fnewt.head()
# for inx in fnew.index:
    # print inx
    # vc=fnewt[inx].value_counts()
    # print vc
    # for inc in vc.index:
        # print 'is'+str(inc)
        # fnew['is'+str(inc)].replace(inx,vc[inc],inplace=True)
# fdf.to_csv('/Users/ufenqi/Downloads/traindfdel2.csv', index=False)
# print fnew.describe().T

        # colser=fdf[col].values.tolist()



# 单变量统计绘图
# sns.distplot(fdf['2'])
#
# # 双变量统计绘图
# sns.pairplot(fdf,vars=['2','66'])
# # 双变量分布绘图
# sns.pairplot(fdf,vars=['2','66'],hue='1',diag_kind='hist')
#
# # 等宽分箱
# fdf['3_1']=pd.qcut(fdf['3'],15)
#
# # 分箱后编码
# dict=enumerate(set(list(fdf['3_1'])))
# for k,v in dict:
#     fdf['3_1'].replace(v,k,inplace=True)
#
# # 交叉表
# def percConvert(ser):
#     return ser/float(ser[-1])
# a=pd.crosstab(fdf['2'],fdf['1'],margins=True).apply(percConvert,axis=1)
# # 百分比显示
# a[0.0].sort_values()
#
# fdf.to_csv('/Users/ufenqi/Downloads/fea_fenxiang.csv',index=False)
#
#
#

