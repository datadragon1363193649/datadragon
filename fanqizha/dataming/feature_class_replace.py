# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
'''
类别型字段做编码处理
'''
def train(dfname):
    UID = dconf.uid
    TARGET = dconf.target
    f_file = dconf.data_path + dfname
    out_name = 'class' + dfname
    out_file = dconf.data_path + out_name
    # 输出参数文件
    json_file = dconf.config_path + 'class_coding.json'
    s_file =  dconf.config_path+dconf.featurename
    # 已知是类别型特征的名字
    class_feature_name=dconf.class_feature_name
    feanamedic={}
    fdf = pd.read_csv(f_file)
    if TARGET in fdf.columns.tolist():
        flist = [UID,TARGET]
    else:
        flist =[UID]
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            flist.append(linelist[0])
            feanamedic[linelist[0]] = linelist[1]

    fdf = fdf[flist]
    fdf = fdf.where(fdf.notnull(), -1)
    columns=fdf.columns.tolist()
    columns.remove(UID)
    if TARGET in columns:
        columns.remove(TARGET)
    tiandic={}
    wfp = open(json_file, 'w')
    for col in class_feature_name:
        fdf[col]=fdf[col].astype('str')
        classcol= fdf[col].unique().tolist()
        classcol.sort()
        # print classcol
        # 获取不同值
        classid=[]
        for ci in range(len(classcol)):
            classid.append(str(ci))
            # print ci,classcol[ci]
        #编码替换原始值
        if len(classcol)>0:
            fdf[col].replace(classcol, classid, inplace=True)
        # 原始值和编码写入参数文件
        print classcol
        for i in range(len(classcol)):
            tiandic.setdefault(feanamedic[col],{})
            print col,classcol[i],classcol[i].encode('utf-8')
            tiandic[feanamedic[col]][classcol[i].encode('utf-8')]=classid[i]
    json.dump(tiandic,wfp)
    for col in columns:
        fdf[col].replace('None', '-1', inplace=True)
        fdf[col] = fdf[col].astype('float64')
    # print fdf.dtypes
    fdf.to_csv(out_file, index=False)
    return out_name
def test(dfname):
    UID = dconf.uid
    TARGET = dconf.target
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
    if TARGET in testdf.columns.tolist():
        flist = [UID,TARGET]
    else:
        flist =[UID]
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            flist.append(linelist[0])
            feanamedic[linelist[0]]=linelist[1]
    testdf=testdf[flist]
    columns = testdf.columns.tolist()
    columns.remove(UID)
    # print columns
    if TARGET in columns:
        columns.remove(TARGET)
    with open(json_file , 'r') as fp:
        c = fp.readline()
        c = json.loads(c)
    print c
    # print testdf.dtypes
    print len(testdf)
    for col in class_feature_name:
        classcol=[]
        classid = []
        if col in feanamedic:
            testdf[col]=testdf[col].astype('string')
            for cvalue in c[feanamedic[col]]:
                classcol.append(cvalue)
                classid.append(c[feanamedic[col]][cvalue])
            testdf[col].replace(classcol, classid, inplace=True)
            testdf1=testdf[testdf[col].isin(classid)]
            t2df=testdf.drop(testdf1.index)
            # print len(t2df)
            # 参数文件中有遗漏的值统一替换为0
            for uid in t2df[UID].values.tolist():
                testdf[col][testdf[UID]==uid]=0
    # testdf['469'].replace('None', '-1', inplace=True)
    print len(testdf)
    for col in columns:
        # print col,feanamedic[col]
        testdf[col].replace('None', '-1', inplace=True)
        testdf[col] = testdf[col].astype('float64')
    testdf.to_csv(out_file, index=False)
    return out_name
if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)