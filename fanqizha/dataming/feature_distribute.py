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
    out_name = 'del' + dfname
    out_file = dconf.data_path + out_name
    json_file = dconf.config_path + 'ceiling.json'
    s_file =  dconf.config_path+dconf.featurename
    feaidlist=[]
    feanamedic={}
    flist = ['0','1']
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            flist.append(linelist[0])
            feanamedic[linelist[0]] = linelist[1]
    fdf = pd.read_csv(f_file)
    fdf = fdf[flist]
    # fdf['39u']=1.0*fdf['39']/(fdf['41']+1)
    fdf=fdf.round(10)
    columns=fdf.columns.tolist()
    columns.remove('0')
    columns.remove('1')
    # columns.remove('2')
    # columns.remove('63')
    jsonc_file = dconf.config_path + 'classfeaname.json'
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    for feid in feanamedic:
        if feanamedic[feid] in cc:
            columns.remove(feid)
        elif feid in dconf.class_feature_name:
            columns.remove(feid)

    # for ci in c:
    #     if
    #     if linelist[0] in ['2','63']:
    #         continue
    #     columns.remove(linelist[0])
    quancol={}
    dflen=len(fdf)
    tiandic={}
    wfp = open(json_file, 'w')
    for col in columns:
        vvl = []
        vvr=[]
        vll=list(set(fdf[col].values.tolist()))
        for vl in vll:
            fmin = fdf[col].quantile(0.0001)
            fmax = fdf[col].quantile(0.9999)
            if vl < fmin:
                vvl.append(vl)
                vvr.append(fmin)
            if vl > fmax:
                vvl.append(vl)
                vvr.append(fmax)
        if len(vvl)!=0:
            fdf[col].replace(vvl,vvr,inplace=True)
        tiandic[feanamedic[col]]=[round(fmin,10),round(fmax,10)]
    json.dump(tiandic,wfp)
    fdf.to_csv(out_file, index=False)
    return out_name
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
            testdf[feid] = testdf[feid].astype('string')
            testdf[feid][testdf[feid] == str(cc[feanamedic[feid]])] = 'no'
            testdf[feid][testdf[feid] != 'no'] = '1'
            testdf[feid][testdf[feid] == 'no'] = '0'
            testdf[feid] = testdf[feid].astype('float64')
            columns.remove(feid)
        elif feid in dconf.class_feature_name:
            columns.remove(feid)



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
# print quancol


# 单变量统计绘图
# sns.distplot(fdf['2'])
#
# # 双变量统计绘图
# sns.pairplot(fdf,vars=['2','66'])
# # 双变量分布绘图
# sns.pairplot(fdf,vars=['2','66'],hue='1',diag_kind='hist')
#
# # 等宽分箱


#
# # 分箱后编码
# dict=enumerate(set(list(fdf['3_1'])))
# for k,v in dict:
#     fdf['3_1'].replace(v,k,inplace=True)
#
# # 交叉表
# def percConvert(ser):
#     return ser/float(ser[-1])
# a=pd.crosstab(fdf['2'],fdf['1'],margins=True).apply(percConvert,axis=1)
# # 百分比显示
# a[0.0].sort_values()
#
# fdf.to_csv('/Users/ufenqi/Downloads/fea_fenxiang.csv',index=False)
#
#
#

