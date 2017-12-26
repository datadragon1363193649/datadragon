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
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_codeinner_10.csv'
    xout_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindataend_x'
    yout_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindataend_y'
    traincol_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traincol'
    fdf = pd.read_csv(f_file)
    fdf['1'].replace([1.0, 0.0], [0.0, 1.0], inplace=True)
    use_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/callfeaturenamecode'
    linelist = []
    with open(use_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
    linelist1 = []
    for lin in linelist:
        if 'fen' not in lin:
            linelist1.append(lin)
    target = '1'
    IDcol = '0'
    # predictors = [x for x in xfdf.columns if x not in [target, IDcol]]
    predictors = linelist
    fdf=fdf[predictors]
    columns=fdf.columns.tolist()
    columns.remove('1')
    columns.remove('0')
    columns.remove('2')
    # colnew.append('0')
    # colnew.append('1')
    # colnew.append('2')
    cwfp = open(traincol_file, 'w')
    cwfp.write('\t'.join(columns))
    # fnew=fdf[colnew]
    final_output=lb.fit_transform(fdf['2'])
    # dflist=list(set(fdf['2']))
    # onthotlist=lb.transform(dflist)

    for col in columns:
        final_output= np.hstack((final_output,lb.fit_transform(fdf[col])))
    # print len(final_output[0])
    # np.savetxt("/Users/ufenqi/Downloads/traindataend.csv", final_output)
    xwfp = open(xout_file, 'w')
    ywfp = open(yout_file, 'w')

    ylist=fdf['1'].values.tolist()
    yuserid=fdf['0'].values.tolist()
    for i in range(0,len(ylist)):
        ywfp.write(str(yuserid[i])+'\t'+str(ylist[i])+'\n')
    ywfp.close()
    for i in range(0,len(final_output)):
        outlist=[]
        for j in final_output[i]:
            outlist.append(str(j))
        xwfp.write(str(yuserid[i])+'\t'+'\t'.join(outlist) +'\n')
    xwfp.close()
def testonehot():
    lb = preprocessing.LabelBinarizer()
    x_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_codeinner_10.csv'
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code4-3_10.csv'
    xout_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata4-3_x'
    yout_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata4-3_y'
    xfdf = pd.read_csv(x_file)
    xfdf['1'].replace([1.0, 0.0], [0.0, 1.0], inplace=True)

    fdf = pd.read_csv(f_file)
    # fdf['1'].replace([1.0, 0.0], [0.0, 1.0], inplace=True)
    fdf = fdf[fdf['1'] > -1]
    fdf['1'][(fdf['1'] > -1) & (fdf['1'] < 30)] = 1
    fdf['1'][fdf['1'] != 1] = 0
    use_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/callfeaturenamecode'
    linelist = []
    with open(use_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
    linelist1 = []
    for lin in linelist:
        if 'fen' not in lin:
            linelist1.append(lin)
    target = '1'
    IDcol = '0'
    predictors = [x for x in xfdf.columns if x not in [target, IDcol]]
    predictors = linelist
    xfdf=xfdf[predictors]
    fdf=fdf[predictors]
    lb.fit_transform(xfdf['2'])
    final_output = lb.transform(fdf['2'])
    # colnew = []
    columns = xfdf.columns.tolist()
    columns.remove('1')
    columns.remove('0')
    columns.remove('2')
    # colnew.append('0')
    # colnew.append('1')
    # colnew.append('2')
    # colnew.append('65')
    for col in columns:
        lb.fit_transform(xfdf[col])
        final_output= np.hstack((final_output,lb.transform(fdf[col.split('_')[0]])))
    # print len(final_output[0])
    # np.savetxt("/Users/ufenqi/Downloads/traindataend.csv", final_output)
    xwfp = open(xout_file, 'w')
    ywfp = open(yout_file, 'w')

    ylist = fdf['1'].values.tolist()
    yuserid = fdf['0'].values.tolist()
    for i in range(0,len(ylist)):
        ywfp.write(str(yuserid[i])+'\t'+str(ylist[i])+'\n')
    ywfp.close()
    for i in range(0,len(final_output)):
        outlist=[]
        for j in final_output[i]:
            outlist.append(str(j))
        xwfp.write(str(yuserid[i])+'\t'+'\t'.join(outlist) +'\n')
    xwfp.close()
if __name__ == '__main__':
    # trainonehot()
    testonehot()
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

