# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

def traincsv():
    # 原始数据
    f_file = '/Users/ufenqi/Documents/dataming/base1/data/userfeatureall3_4'
    s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
    feaidlist = []
    feanamelist = []
    feaidlist.append('0')
    feaidlist.append('1')
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
    print len(feaidlist)
    print feaidlist
    ff =[]
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    fdf.columns = feaidlist
    columns = fdf.columns.tolist()
    print len(columns)
    # print columns
    # print fdf[2].unique().counts()
    # for col in columns[2:]:
    #     if len(fdf[col].unique()) < 2:
    #         columns.remove(col)
    # print len(columns)
    print columns

    # print columns
    fnewd = fdf[columns]
    columns.remove('0')
    for col in columns[1:]:
        fnewd[col] = fnewd[col].astype('float64')
    # fnewd=fnewd[1400:]

    # print len(colnew)
    # fnewd.columns=colnew
    fnewd.to_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeatureall3_4.csv',index=False)
    # fdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/traindata01.csv')
    #
    # columns=fdf.columns.tolist()
    # dflen=len(fdf)
    # print columns
    # colen=[]
    # for col in columns:
    #     print col
    #     print fdf[col].value_counts()
    #     colmaxlen=fdf[col].value_counts().values.tolist()[0]
    #     if colmaxlen<(dflen*0.8):
    #         colen.append(col)
    # print colen
    # print len(colen)
    # # print columns
    # fnewd=fdf[colen]
    # f_file = '/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/test_all_bin'
    # ff = []
    # with open(f_file, 'r') as fp:
    #     for line in fp:
    #         linelist = line.strip().split('\t')
    #         ff.append(linelist)
    # testdf = pd.DataFrame(ff)
    # testdf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testdatafenxiang.csv', index=False)
    # testdf = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testdatafenxiang.csv')
    # testnewd = testdf[colen]
    # for col in colen:
    #     fnewd[col]=fnewd[col].astype('int')
    #     testnewd[col] = testnewd[col].astype('int')
    # # corr=fnewd.corr(method='spearman').ix[66].abs()
    # # corr1=corr[corr.values>=0.05]
    # # print corr1.sort_values()
    # # indexlist=corr1.index
    # # indexlist=list(indexlist)
    # # columns=[int(i) for i in indexlist]
    # # # columns.remove(56)
    # # fnewd01=fnewd[columns]
    #
    # testnewd.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testdatafenxiang1.csv', index=False)
    #
    # fnewd.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/traindatafenxiang1.csv',index=False)
    # # print fnewd01.describe(include='all').T


    # columns=fdf.columns
    # print columns
    # columnsmiss=[]
    # for col in columns:
    #     print '字段：'+str(col)
    #     print fdf[col].value_counts()
def testcsv():
    # 原始数据
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeature_4'
    s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
    feaidlist = []
    feanamelist = []
    feaidlist.append('0')
    # feaidlist.append('1')
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
    print len(feaidlist)
    print feaidlist
    ff = []
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    fdf.columns = feaidlist
    columns = fdf.columns.tolist()
    print len(columns)
    # print columns
    # print fdf[2].unique().counts()
    # for col in columns[2:]:
    #     if len(fdf[col].unique()) < 2:
    #         columns.remove(col)
    # print len(columns)
    print columns

    # print columns
    fnewd = fdf[columns]
    columns.remove('0')
    for col in columns[1:]:
        fnewd[col] = fnewd[col].astype('float64')
    fnewd.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeature_4.csv', index=False)
    # fdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/traindata01.csv')
    #
    # columns=fdf.columns.tolist()
    # dflen=len(fdf)
    # print columns
    # colen=[]
    # for col in columns:
    #     print col
    #     print fdf[col].value_counts()
    #     colmaxlen=fdf[col].value_counts().values.tolist()[0]
    #     if colmaxlen<(dflen*0.8):
    #         colen.append(col)
    # print colen
    # print len(colen)
    # # print columns
    # fnewd=fdf[colen]
    # f_file = '/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/test_all_bin'
    # ff = []
    # with open(f_file, 'r') as fp:
    #     for line in fp:
    #         linelist = line.strip().split('\t')
    #         ff.append(linelist)
    # testdf = pd.DataFrame(ff)
    # testdf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testdatafenxiang.csv', index=False)
    # testdf = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testdatafenxiang.csv')
    # testnewd = testdf[colen]
    # for col in colen:
    #     fnewd[col]=fnewd[col].astype('int')
    #     testnewd[col] = testnewd[col].astype('int')
    # # corr=fnewd.corr(method='spearman').ix[66].abs()
    # # corr1=corr[corr.values>=0.05]
    # # print corr1.sort_values()
    # # indexlist=corr1.index
    # # indexlist=list(indexlist)
    # # columns=[int(i) for i in indexlist]
    # # # columns.remove(56)
    # # fnewd01=fnewd[columns]
    #
    # testnewd.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testdatafenxiang1.csv', index=False)
    #
    # fnewd.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/traindatafenxiang1.csv',index=False)
    # # print fnewd01.describe(include='all').T


    # columns=fdf.columns
    # print columns
    # columnsmiss=[]
    # for col in columns:
    #     print '字段：'+str(col)
    #     print fdf[col].value_counts()
if __name__ == '__main__':
    traincsv()
    # testcsv()

