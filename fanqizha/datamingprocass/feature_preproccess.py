# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

def trainpre():
    # 原始数据
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/fea_new_merge_refuse'
    ff =[]
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    columns=fdf.columns.tolist()
    print len(columns)

    for col in columns[2:]:
        if len(fdf[col].unique())<2:
            columns.remove(col)
    print len(columns)
    print columns

    # print columns
    fnewd=fdf[columns]
    for col in columns:
        fnewd[col]=fnewd[col].astype('float64')
    fnewd.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata02.csv',index=False)
def testpre():
    trainfile='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata03.csv'
    testfile='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/fea_new_merge_testreal'
    stype_file='/Users/ufenqi/Downloads/SourceType.txt'
    stydic={}
    with open(stype_file, 'r') as fps:
        for line in fps:
            linelist = line.strip().split(',')
            # print len(linelist)
            sty=linelist[1]
            # print linelist[0],linelist[1
            if not linelist[1]:
                # print 111
                sty=-1
            if linelist[1] == 4 or linelist[1] == 5:
                sty = 2
            if linelist[1] == None:
                sty = 9
            stydic[linelist[0]]=linelist[1]
    ff = []
    traindf = pd.read_csv(trainfile)
    columnlist=traindf.columns.tolist()
    # print len(columnlist)
    with open(testfile, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            le=linelist[:-1]
            le.append(stydic[linelist[0]])
            # print len(linelist)
            ff.append(le)
    fdf = pd.DataFrame(ff)
    fcolumns=fdf.columns.tolist()
    fcolumns=fcolumns[:56]
    fdf=fdf[fcolumns]
    fdf.columns=columnlist
    # print fdf.head()
    # print len(fdf.columns)
    columnlist.remove('0')
    for col in columnlist:
        # print fdf[col].value_counts()
        fdf[col] = fdf[col].astype('float64')
    fdf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testreal02.csv', index=False)

def csvtotxt():
    trainfile = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata03.csv'
    toutfile='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata'
    trainfile = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testreal02.csv'
    toutfile = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata'

if __name__ == '__main__':
    testpre()