# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
def percConvert(ser):
    return ser/float(ser[-1])
def train(dfname):

    # 原始数据
    f_file = dconf.data_path + dfname
    out_name = 'code' + dfname
    out_file = dconf.data_path + out_name
    # out_file = dconf.data_path + 'code' + dfname
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
    columnslist.remove('0')
    columnslist.remove('1')
    # columnslist.remove('2')
    # columnslist.remove('63')
    jsonc_file = dconf.config_path + 'classfeaname.json'
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    for feid in feanamedic:
        if feanamedic[feid] in cc:
            columnslist.remove(feid)
        elif feid in dconf.class_feature_name:
            columnslist.remove(feid)
    dflen=fdf.__len__()
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
    # tndf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturecode.csv',index=False)
    wfp.close()
    return out_name
def test(dfname):
    # 原始数据
    test_file = dconf.data_path + dfname
    out_name = 'code' + dfname
    out_file = dconf.data_path + out_name
    # out_file = dconf.data_path + 'code' + dfname
    json_file = dconf.config_path + 'binning.json'
    s_file = dconf.config_path + dconf.featurename
    feaidlist = []
    feaidlist.append('0')
    # feaidlist.append('1')
    feanamedic = {}
    flist=['0']
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            flist.append(linelist[0])

            feanamedic[linelist[0]] = linelist[1]
    noridlist=[]
    # with open(nor_file, 'r') as fp:
    #     for line in fp:
    #         norfealist = line.strip().split(',')
    #         noridlist.append(norfealist[0])
    #         nordic[norfealist[0]]=norfealist[1]
    #         if norfealist[0]=='0' or norfealist[0]=='2'or norfealist[0]=='63':
    #             wfpc.write(norfealist[0] + ',' + norfealist[1] + '\n')
    #         else:
    #             wfpc.write(norfealist[0]+'fen10'+','+norfealist[1]+'\n')
            # codedic[norfealist[0]+'fen10']=norfealist[1]
    # wfp = open(fenxiang1out_file, 'w')

    testdf = pd.read_csv(test_file)
    testdf=testdf[flist]
    testdf = testdf.round(10)
    # testdf=testdf[240000:]

    # testdf=testdf[feaidlist]

    columnslist = testdf.columns.tolist()
    columnslist.remove('0')
    if '1' in columnslist:
        columnslist.remove('1')
    # columnslist.remove('2')
    # columnslist.remove('63')
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
                # print 'aaa',tvl,len(arrylist) - 2
                su += 1
                continue
            bol=0
            for j in range(1, len(arrylist)):
                if tvl <= arrylist[j]:
                    dfl.append(j - 1)
                    # print 'bbb', tvl, j - 1
                    bol=1
                    break
            if bol==0:
                print tvl
            su+=1
        if len(dfl) != 0:
            testdf[col].replace(testvll, dfl, inplace=True)
    testdf.to_csv(out_file, index=False)
    return out_name
def code10():
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnorinner.csv'
    test_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdfnorinner.csv'
    fenxiang1out_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/fenxiang11'
    numbercnt_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/numbercnt11'

    wfp = open(fenxiang1out_file, 'w')
    nwfp = open(numbercnt_file, 'w')
    fdf = pd.read_csv(f_file)
    testdf = pd.read_csv(test_file)
    columnslist = fdf.columns.tolist()
    columnslist.remove('0')
    columnslist.remove('1')
    columnslist.remove('2')
    columnslist.remove('63')
    numcol = []
    numfen = []
    nonumcol = []
    nonumcol.append('0')
    nonumcol.append('1')
    nonumcol.append('2')
    nonumcol.append('63')
    nonfen = []
    nonfen.append('0')
    nonfen.append('1')
    nonfen.append('2')
    nonfen.append('63')
    dflen = fdf.__len__()

    for col in columnslist:
        fdf[col], arrylist = pd.qcut(fdf[col], 10, labels=False, retbins=True, duplicates='drop')
        colmaxlen = fdf[col].value_counts().values.tolist()[0]
        # if colmaxlen > (dflen * 0.8):
        #     del fdf[col]
        #     continue
        strlist = []
        for i in range(0, len(arrylist)):
            strlist.append(str(arrylist[i]))
        if len(arrylist) == 11:
            numcol.append(col)
            numfen.append(col + 'fen1')
        else:
            nonumcol.append(col)
            nonfen.append(col + 'fen1')
        wfp.write(col + '\t' + '\t'.join(strlist) + '\n')
        testvll = list(set(testdf[col].values.tolist()))
        dfl = []
        for tvl in testvll:
            if tvl > arrylist[-2]:
                dfl.append(len(arrylist) - 2)
                continue
            for j in range(1, len(arrylist)):
                if tvl <= arrylist[j]:
                    dfl.append(j - 1)
                    break
        if len(dfl) != 0:
            testdf[col].replace(testvll, dfl, inplace=True)
    nwfp.write(str(len(nonumcol)))
    nwfp.close()
    nonumcol += numcol
    ndf = fdf[nonumcol]
    tndf = testdf[nonumcol]
    nonfen += numfen
    ndf.columns = nonfen
    tndf.columns = nonfen

    ndf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_codeinner_10.csv', index=False)
    tndf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_codeinner_10.csv', index=False)
    wfp.close()

    # traincode1=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_code1.csv')
    # testcode1 = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code1.csv')
    #
    # traincode2 = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
    # testcode2 = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode.csv')
    #
    # del traincode1['1']
    # del traincode1['2']
    # del traincode1['63']
    # traincode3 = pd.merge(traincode2, traincode1, on=['0', '0'], how='left')
    # del testcode1['1']
    # del testcode1['2']
    # del testcode1['63']
    # testcode3 = pd.merge(testcode2, testcode1, on=['0', '0'], how='left')
    #
    # traincode3.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode1.csv', index=False)
    # testcode3.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode1.csv', index=False)

if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)