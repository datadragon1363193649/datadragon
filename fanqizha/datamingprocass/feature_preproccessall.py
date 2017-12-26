# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

def traincsv():
    # 原始数据
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testconandcredit'
    ff =[]
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    colnew = [0]
    colnew += range(67, 107)
    print colnew
    colnew.remove(77)
    colnew.remove(79)
    fdf.columns = colnew
    columns = fdf.columns.tolist()
    print len(columns)
    # print columns
    # print fdf[2].unique().counts()
    # for col in columns[2:]:
    #     if len(fdf[col].unique()) < 2:
    #         columns.remove(col)
    print len(columns)
    print columns

    # print columns
    fnewd = fdf[columns]
    columns.remove(0)
    for col in columns:
        fnewd[col] = fnewd[col].astype('float64')

    # print len(colnew)
    # fnewd.columns=colnew
    fnewd.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testconandcredit.csv',index=False)
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
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testconandcredituser'
    traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trianconandcredit.csv')
    traincol=traindf.columns.tolist()
    ff = []
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    colnew = [0]
    colnew += range(67, 107)
    print colnew
    colnew.remove(77)
    colnew.remove(79)
    colstr=[]
    for col in colnew:
        colstr.append(str(col))
    fdf.columns = colstr
    fdf=fdf[traincol]
    traincol.remove('0')
    for col in traincol:
        fdf[col] = fdf[col].astype('float64')
    fdf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testconandcreditreal.csv', index=False)
if __name__ == '__main__':
    # traincsv()
    testcsv()

