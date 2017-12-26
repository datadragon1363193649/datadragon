# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
def train(dfname):
    # 原始数据
    # f_file = '/Users/ufenqi/Documents/dataming/base1/data/traindataall3.csv'
    # out_file='/Users/ufenqi/Documents/dataming/base1/config/ceiling.json'
    f_file = dconf.data_path + dfname
    out_name = 'class' + dfname
    out_file = dconf.data_path + out_name
    json_file = dconf.config_path + 'class_coding.json'
    s_file =  dconf.config_path+dconf.featurename
    class_feature_name=dconf.class_feature_name
    feaidlist=[]
    feanamedic={}
    fdf = pd.read_csv(f_file)
    if '1' in fdf.columns.tolist():
        flist = ['0','1']
    else:
        flist =['0']
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            flist.append(linelist[0])
            feanamedic[linelist[0]] = linelist[1]

    fdf = fdf[flist]
    fdf = fdf.where(fdf.notnull(), -1)
    # fdf['39u']=1.0*fdf['39']/(fdf['41']+1)
    # fdf=fdf.round(10)
    columns=fdf.columns.tolist()
    columns.remove('0')
    if '1' in columns:
        columns.remove('1')
    quancol={}
    dflen=len(fdf)
    tiandic={}
    wfp = open(json_file, 'w')
    for col in class_feature_name:
        classcol= fdf[col].unique().tolist()
        classcol.sort()
        classid=[]
        for ci in range(len(classcol)):
            classid.append(str(ci))
            # print ci,classcol[ci]
        if len(classcol)>0:
            fdf[col].replace(classcol, classid, inplace=True)
        for i in range(len(classcol)):
            tiandic.setdefault(feanamedic[col],{})
            tiandic[feanamedic[col]][classcol[i]]=classid[i]
    json.dump(tiandic,wfp)
    # fdf['469'].replace('None', '-1', inplace=True)
    # print fdf['469'].value_counts()
    for col in columns:
        fdf[col].replace('None', '-1', inplace=True)
        fdf[col] = fdf[col].astype('float64')
    print fdf.dtypes
    fdf.to_csv(out_file, index=False)
def test(dfname):
    # 原始数据
    test_file = dconf.data_path+dfname
    out_name='class'+dfname
    out_file=dconf.data_path+out_name
    json_file = dconf.config_path +'class_coding.json'
    testdf = pd.read_csv(test_file)
    s_file = dconf.config_path + dconf.featurename
    class_feature_name = dconf.class_feature_name
    feaidlist = []
    feanamedic ={}
    # feaidlist.append('0')
    if '1' in testdf.columns.tolist():
        flist = ['0','1']
    else:
        flist =['0']
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            flist.append(linelist[0])
            feanamedic[linelist[0]]=linelist[1]
    testdf=testdf[flist]
    columns = testdf.columns.tolist()
    columns.remove('0')
    # print columns
    if '1' in columns:
        columns.remove('1')
    with open(json_file , 'r') as fp:
        c = fp.readline()
        c = json.loads(c)
    print testdf.dtypes
    for col in class_feature_name:
        classcol=[]
        classid = []
        if col in feanamedic:
            testdf[col]=testdf[col].astype('string')
            print col,feanamedic[col]
            # print testdf[col].unique().tolist()
            for cvalue in c[feanamedic[col]]:
                # print cvalue,c[feanamedic[col]][cvalue]
                classcol.append(cvalue)
                classid.append(c[feanamedic[col]][cvalue])

            testdf[col].replace(classcol, classid, inplace=True)
            print '***************'
    # testdf['469'].replace('None', '-1', inplace=True)
    for col in columns:
        print col,feanamedic[col]
        testdf[col].replace('None', '-1', inplace=True)
        testdf[col] = testdf[col].astype('float64')
    testdf.to_csv(out_file, index=False)
if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)