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
对数据做log处理和归一化处理
'''
def train(dfname):
    # log
    def loghandle(dfname):
        UID = dconf.uid
        TARGET = dconf.target
        int_file=dconf.data_path + dfname
        out_file = dconf.data_path + 'log' + dfname
        traindatadf = pd.read_csv(int_file)
        json_file =dconf.config_path + 'loginvalue.json'
        s_file = dconf.config_path + dconf.featurename
        feaidlist = []
        feanamedic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamedic[linelist[0]] = linelist[1]
        wfp = open(json_file, 'w')
        feacol = traindatadf.columns.tolist()
        feacol.remove(UID)
        feacol.remove(TARGET)

        # 删除类别型，类别型不做log处理
        jsonc_file = dconf.config_path + 'classfeaname.json'
        with open(jsonc_file, 'r') as fp:
            cc = fp.readline()
            cc = json.loads(cc)
        for feid in feanamedic:
            if feanamedic[feid] in cc:
                feacol.remove(feid)
            elif feid in dconf.class_feature_name:
                feacol.remove(feid)
        logdic = {}
        for col in feacol:
            logdic[feanamedic[col]] = round(traindatadf[col].min(), 10)
            # 负值不能做log处理
            a = traindatadf[col] - traindatadf[col].min()
            traindatadf[col] = np.log(a + 1)
            if len(traindatadf[col].unique()) < 2:
                continue

        json.dump(logdic, wfp)
        trainredf = traindatadf.round(10)
        wfp.close()
        trainredf.to_csv(out_file,index=False)
        return trainredf

    # 归一化
    def normalizing(traindatadf):
        UID = dconf.uid
        TARGET = dconf.target
        json_file = dconf.config_path + 'norinvalue.json'
        out_name = 'nor' + dfname
        out_file = dconf.data_path + out_name
        s_file = dconf.config_path +  dconf.featurename
        feaidlist = []
        feanamedic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamedic[linelist[0]] = linelist[1]
        wfp = open(json_file, 'w')
        feacol = traindatadf.columns.tolist()
        feacol.remove(UID)
        feacol.remove(TARGET)

        # 删除类别型，类别型不做归一化处理
        jsonc_file = dconf.config_path + 'classfeaname.json'
        with open(jsonc_file, 'r') as fp:
            cc = fp.readline()
            cc = json.loads(cc)
        for feid in feanamedic:
            if feanamedic[feid] in cc:
                feacol.remove(feid)
            elif feid in dconf.class_feature_name:
                feacol.remove(feid)
        nordic = {}
        numv=0.8*len(traindatadf)
        for col in feacol:
            nordic[feanamedic[col]] = [round(traindatadf[col].min(), 10), round(traindatadf[col].max(), 10)]
            a1 = traindatadf[col] - traindatadf[col].min()
            a2 = traindatadf[col].max() - traindatadf[col].min()
            traindatadf[col] = a1 / a2
            tvc=traindatadf[col].value_counts()
            if tvc[0]>numv:
                traindatadf[col][traindatadf[col]==tvc.index[0]]=0
                traindatadf[col]
        json.dump(nordic, wfp)
        trainredf = traindatadf.round(10)
        trainredf.to_csv(out_file,index=False)
        return out_name
    traindf = loghandle(dfname)
    return normalizing(traindf)
def test(dfname):
    def loghandle(dfname):
        UID = dconf.uid
        TARGET = dconf.target
        test_file = dconf.data_path + dfname
        out_file = dconf.data_path + 'log' + dfname
        json_file = dconf.config_path + 'loginvalue.json'
        s_file = dconf.config_path + dconf.featurename
        testdatadf = pd.read_csv(test_file)
        feaidlist = []

        if TARGET in testdatadf.columns.tolist():
            flist = [UID, TARGET]
        else:
            flist = [UID]
        feanamedic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                flist.append(linelist[0])
                feanamedic[linelist[0]]=linelist[1]
        testdatadf=testdatadf[flist]
        with open(json_file, 'r') as fp:
            c = fp.readline()
            c = json.loads(c)
        feacol=feaidlist
        if TARGET in feacol:
            feacol.remove(TARGET)
        # 删除类别型，类别型不做log处理
        jsonc_file = dconf.config_path + 'classfeaname.json'
        with open(jsonc_file, 'r') as fp:
            cc = fp.readline()
            cc = json.loads(cc)
        for feid in feanamedic:
            if feanamedic[feid] in cc:
                feacol.remove(feid)
            elif feid in dconf.class_feature_name:
                feacol.remove(feid)

        recoldic={}
        for col in feacol:
            recoldic[col] = feanamedic[col]
            minvalue=c[feanamedic[col]]
            t=testdatadf[col]-minvalue
            testdatadf[col] = np.log(t + 1)

        testredf = testdatadf.round(10)
        testredf.to_csv(out_file, index=False)
        return recoldic,testdatadf
    #归一化
    def normalizing(redic,testdatadf):
        UID = dconf.uid
        TARGET = dconf.target
        out_name = 'nor' + dfname
        out_file = dconf.data_path + out_name
        json_file = dconf.config_path + 'norinvalue.json'
        with open(json_file, 'r') as fp:
            c = fp.readline()
            c = json.loads(c)
        wfp = open(out_file, 'w')
        s_file = dconf.config_path + dconf.featurename
        feaidlist = []
        feanamedic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamedic[linelist[0]] = linelist[1]
        feacol = testdatadf.columns.tolist()
        feacol.remove(UID)
        if TARGET in feacol:
            feacol.remove(TARGET)
        # 删除类别型，类别型不做归一化处理
        jsonc_file = dconf.config_path + 'classfeaname.json'
        with open(jsonc_file, 'r') as fp:
            cc = fp.readline()
            cc = json.loads(cc)
        for feid in feanamedic:
            if feanamedic[feid] in cc:
                feacol.remove(feid)
            elif feid in dconf.class_feature_name:
                feacol.remove(feid)

        for col in feacol:
            t1=testdatadf[col] - c[redic[col]][0]
            a2 = c[redic[col]][1] -c[redic[col]][0]
            testdatadf[col] = t1 / a2
        testredf = testdatadf.round(10)
        testredf.to_csv(out_file, index=False)
        return out_name

    redic ,testdf= loghandle(dfname)
    return normalizing(redic,testdf)

if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)
