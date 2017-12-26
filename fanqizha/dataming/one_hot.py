# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import numpy as np
import offline_db_conf as dconf
import json
reload(sys)
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
    fdf = fdf.round(10)
    columnslist = fdf.columns.tolist()
    columnslist.remove('0')
    columnslist.remove('1')
    # columnslist.remove('2')
    # columnslist.remove('63')
    jsonc_file = dconf.config_path + 'classfeaname.json'
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    one_col_dic=['0',]
    other_col_dic = ['0','1']
    for feid in feanamedic:
        if feanamedic[feid] not in cc and feid in dconf.class_feature_name:
            one_col_dic.append(feid)
        else:
            other_col_dic.append(feid)
    onedf=fdf[one_col_dic]
    for onecol in onedf:
        onedf[onecol]=onedf[onecol].astype('string')
    onedf2=pd.get_dummies()
def test(dfname):
    # 原始数据
    test_file = dconf.data_path+dfname
    out_name='del'+dfname
    out_file=dconf.data_path+out_name
    json_file = dconf.config_path +'ceiling.json'
    testdf = pd.read_csv(test_file)
    # testdf['39u'] = 1.0 * testdf['39'] / (testdf['41']+1)
    testdf = testdf.round(10)
    s_file = dconf.config_path + dconf.featurename
    feaidlist = []
    feanamedic ={}
    # feaidlist.append('0')
    flist = ['0']
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            flist.append(linelist[0])
            feanamedic[linelist[0]]=linelist[1]
    columns = feaidlist
    testdf=testdf[flist]
    # print columns
    if '1' in columns:
        columns.remove('1')
    # columns.remove('2')
    # columns.remove('63')
    jsonc_file = dconf.config_path + 'classfeaname.json'
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    for feid in feanamedic:
        if feanamedic[feid] in cc:
            if feid in ['2', '63']:
                continue
            columns.remove(feid)
            testdf[feid] = testdf[feid].astype('string')
            testdf[feid][testdf[feid] == str(cc[feanamedic[feid]])] = 'no'
            testdf[feid][testdf[feid] != 'no'] = '1'
            testdf[feid][testdf[feid] == 'no'] = '0'
            testdf[feid] = testdf[feid].astype('float64')



    # classname_file = dconf.config_path + 'classname'
    # with open(classname_file, 'r') as fp:
    #     for line in fp:
    #         linelist = line.strip().split(',')
    #         if linelist[0] in ['2', '63']:
    #             continue
    #         print linelist[0]
    #         columns.remove(linelist[0])
    #         testdf[linelist[0]] = testdf[linelist[0]].astype('string')
    #         testdf[linelist[0]][testdf[linelist[0]] == linelist[1]] = 'no'
    #         # testdfall['1'][testdfall['1'] != 1] = 0
    #         testdf[linelist[0]][testdf[linelist[0]] != 'no'] = '1'
    #         testdf[linelist[0]][testdf[linelist[0]] == 'no'] = '0'
    #         testdf[linelist[0]] = testdf[linelist[0]].astype('float64')
    with open(json_file , 'r') as fp:
        c = fp.readline()
        c = json.loads(c)
    for col in columns:
        print col
        print testdf[col]
        testvll = list(set(testdf[col].values.tolist()))
        print len(testvll)
        vvl = []
        vvr = []
        for tvl in testvll:
            fmin=c[feanamedic[col]][0]
            fmax=c[feanamedic[col]][1]
            # fmin=c[col][0]
            # fmax=c[col][1]
            if tvl < fmin:
                vvl.append(tvl)
                vvr.append(fmin)
            if tvl > fmax:
                vvl.append(tvl)
                vvr.append(fmax)

        if len(vvl) != 0:
            # print col
            testdf[col].replace(vvl, vvr, inplace=True)
    # json.dump(tiandic, wfp)
    print testdf.head()
    # del testdf['43']
    # del testdf['44']
    # del testdf['45']
    testdf.to_csv(out_file, index=False)
    return out_name
if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)