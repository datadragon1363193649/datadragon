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
对数据进行分箱
'''
def train(dfname):
    UID = dconf.uid
    TARGET = dconf.target
    f_file = dconf.data_path + dfname
    out_name = 'code' + dfname
    out_file = dconf.data_path + out_name
    json_file = dconf.config_path + 'binning.json'
    s_file = dconf.config_path + dconf.featurename
    feaidlist = []
    feanamedic = {}
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamedic[linelist[0]] = linelist[1]
    wfp = open(json_file, 'w')
    fdf = pd.read_csv(f_file)
    fdf=fdf.round(10)
    columnslist=fdf.columns.tolist()
    columnslist.remove(UID)
    columnslist.remove(TARGET)
    jsonc_file = dconf.config_path + 'classfeaname.json'
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    for feid in feanamedic:
        if feanamedic[feid] in cc:
            columnslist.remove(feid)
        elif feid in dconf.class_feature_name:
            columnslist.remove(feid)
    binningdic={}
    for col in columnslist:
        fdf[col],arrylist=pd.qcut(fdf[col],10,labels=False,retbins=True,duplicates='drop')
        strlist=[]
        for i in range(0,len(arrylist)) :
            arrylist[i]=round(arrylist[i],10)
            strlist.append(round(float(arrylist[i]),10))
        binningdic[feanamedic[col]]=strlist
    json.dump(binningdic,wfp)
    fdf.to_csv(out_file,index=False)
    wfp.close()
    return out_name
def test(dfname):
    UID = dconf.uid
    TARGET = dconf.target
    test_file = dconf.data_path + dfname
    out_name = 'code' + dfname
    out_file = dconf.data_path + out_name
    json_file = dconf.config_path + 'binning.json'
    s_file = dconf.config_path + dconf.featurename
    testdf = pd.read_csv(test_file)
    feanamedic = {}
    if TARGET in testdf.columns.tolist():
        flist = [UID, TARGET]
    else:
        flist = [UID]
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            flist.append(linelist[0])
            feanamedic[linelist[0]] = linelist[1]
    testdf=testdf[flist]
    testdf = testdf.round(10)

    columnslist = testdf.columns.tolist()
    columnslist.remove(UID)
    if TARGET in columnslist:
        columnslist.remove(TARGET)
    jsonc_file = dconf.config_path + 'classfeaname.json'
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    for feid in feanamedic:
        if feanamedic[feid] in cc:
            columnslist.remove(feid)
        elif feid in dconf.class_feature_name:
            columnslist.remove(feid)

    with open(json_file, 'r') as fp:
        c = fp.readline()
        c = json.loads(c)
    for col in columnslist:
        arrylist=c[feanamedic[col]]
        for i in range(0,len(arrylist)) :
            arrylist[i]=round(arrylist[i],10)
        testvll = list(set(testdf[col].values.tolist()))
        print col,len(testvll)
        dfl = []
        su=0
        for tvl in testvll:
            if tvl > arrylist[-2]:
                dfl.append(len(arrylist) - 2)
                su += 1
                continue
            bol=0
            for j in range(1, len(arrylist)):
                if tvl <= arrylist[j]:
                    dfl.append(j - 1)
                    bol=1
                    break
            if bol==0:
                print tvl
            su+=1
        if len(dfl) != 0:
            testdf[col].replace(testvll, dfl, inplace=True)
    testdf.to_csv(out_file, index=False)
    return out_name
if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)