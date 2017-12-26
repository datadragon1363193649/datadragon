# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
# pd.set_option('precision', 10)
def train(dfname):
    def loghandle(dfname):
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
        import seaborn as sns
        feacol = traindatadf.columns.tolist()
        feacol.remove('0')
        feacol.remove('1')
        # feacol.remove('2')
        # feacol.remove('63')

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
            # print len(feacol)
            logdic[feanamedic[col]] = round(traindatadf[col].min(), 10)
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
        json_file = dconf.config_path + 'norinvalue.json'
        out_name = 'nor' + dfname
        out_file = dconf.data_path + out_name
        # out_file = dconf.data_path + 'nor' + dfname
        s_file = dconf.config_path +  dconf.featurename
        feaidlist = []
        feanamedic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamedic[linelist[0]] = linelist[1]
        wfp = open(json_file, 'w')
        import seaborn as sns
        feacol = traindatadf.columns.tolist()
        feacol.remove('0')
        feacol.remove('1')
        # feacol.remove('2')
        # feacol.remove('63')
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
            # wfp.write(col + '\t' + str(traindatadf[col].max())+'\t' + str(traindatadf[col].min()) + '\n')
        json.dump(nordic, wfp)
        trainredf = traindatadf.round(10)
        # testredf = testdatadf[recol]
        # testredf = testredf.round(10)
        trainredf.to_csv(out_file,index=False)
        return out_name
        # testredf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturenor.csv', index=False)
    traindf = loghandle(dfname)
    return normalizing(traindf)
def test(dfname):
    def loghandle(dfname):
        # traindatadf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfdelinner.csv')
        # testdatadf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/view_sample_selectiondel.csv')
        # out_file='/Users/ufenqi/Documents/dataming/base1/config/loginvalue.json'

        test_file = dconf.data_path + dfname
        out_file = dconf.data_path + 'log' + dfname
        json_file = dconf.config_path + 'loginvalue.json'
        s_file = dconf.config_path + dconf.featurename
        testdatadf = pd.read_csv(test_file)
        feaidlist = []
        flist=['0']
        feanamedic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                flist.append(linelist[0])
                feanamedic[linelist[0]]=linelist[1]
        columns = feaidlist
        testdatadf=testdatadf[flist]
        with open(json_file, 'r') as fp:
            c = fp.readline()
            c = json.loads(c)
        feacol=feaidlist
        if '1' in feacol:
            feacol.remove('1')
        # feacol.remove('2')
        # feacol.remove('63')

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
        # trainredf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfloginner.csv',index=False)
        testredf.to_csv(out_file, index=False)
        return recoldic,testdatadf
    #归一化
    def normalizing(redic,testdatadf):
        out_name = 'nor' + dfname
        out_file = dconf.data_path + out_name
        # out_file = dconf.data_path + 'nor' + dfname
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
        feacol.remove('0')
        if '1' in feacol:
            feacol.remove('1')
        # feacol.remove('2')
        # feacol.remove('63')

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
        # trainredf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnorinner.csv',index=False)
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
# sns.distplot(traindatadf['5'])
# traindatadf['7log']=np.log(traindatadf['7']+1)
# fu=np.log(abs(traindatadf['8'][traindatadf['8']<0])+1)*-1
# zheng=np.log(traindatadf['8'][traindatadf['8']>=0]+1)
# traindatadf['8log']=fu.append(zheng)

