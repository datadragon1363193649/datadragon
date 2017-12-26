# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')


def train():
    # 原始数据
    # f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindatainner.csv'
    f_file = '/Users/ufenqi/Documents/dataming/base1/data/traindataall3.csv'
    # test_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeature.csv'
    out_file='/Users/ufenqi/Documents/dataming/base1/config/ceiling.json'

    fdf = pd.read_csv(f_file)
    # fdf['39u']=1.0*fdf['39']/(fdf['41']+1)
    fdf=fdf.round(10)
    # testdf = pd.read_csv(test_file)
    # testdf=1.0*testdf['39']/testdf['41']
    # testdf=testdf.round(10)
    columns=fdf.columns.tolist()
    columns.remove('0')
    columns.remove('1')
    columns.remove('2')
    columns.remove('63')

    # s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_my'
    # s_file = '/Users/ufenqi/Downloads/featurename_update'
    # feaidlist = []
    # feanamelist = []
    # feaidlist.append('0')
    # with open(s_file, 'r') as fp:
    #     for line in fp:
    #         linelist = line.strip().split(',')
    #         feaidlist.append(linelist[0])
    #         feanamelist.append(linelist[1])
    # columns=feaidlist
    # columns.remove('2')
    # # columns.remove('19')
    # # columns.remove('43')
    # # columns.remove('45')
    # # columns.remove('54')
    # # columns.remove('55')
    # columns.remove('63')

    quancol={}
    dflen=len(fdf)
    tiandic={}
    wfp = open(out_file, 'w')
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
        # if len(fdf[col].unique()) < 2:
        #     del fdf[col]
        #     # del testdf[col]
        #     continue
        # else:
        tiandic[col]=[round(fmin,10),round(fmax,10)]
            # wfp.write(str(col) + '\t' + str(fdf[col].quantile(0.0001)) + '\t' + str(fdf[col].quantile(0.9999)) + '\n')
        # testvll = list(set(testdf[col].values.tolist()))
        vvl = []
        vvr = []
        # for tvl in testvll:
        #     # fmin=fdf[col].quantile(0.0001)
        #     # fmax=fdf[col].quantile(0.9999)
        #     if tvl <fmin:
        #         vvl.append(tvl)
        #         vvr.append(fmin)
        #     if tvl>fmax:
        #         vvl.append(tvl)
        #         vvr.append(fmax)
        # if len(vvl) != 0:
        #     testdf[col].replace(vvl, vvr, inplace=True)
        # colmaxlen = fdf[col].value_counts().values.tolist()[0]
        # if colmaxlen>(dflen*0.8):
        #     del fdf[col]
    json.dump(tiandic,wfp)
    # fdf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfdelinner.csv',index=False)
    fdf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/traindfdelinner.csv', index=False)
    # testdf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturedel.csv',index=False)
def test():
    # 原始数据
    f_file = '/Users/ufenqi/Documents/dataming/base1/data/traindataall3.csv'
    test_file = '/Users/ufenqi/Documents/dataming/base1/data/userfeature_501.csv'
    out_file = '/Users/ufenqi/Documents/dataming/base1/config/ceiling.json'
    fdf = pd.read_csv(f_file)
    fdf = fdf.round(10)
    testdf = pd.read_csv(test_file)
    testdf['39u'] = 1.0 * testdf['39'] / (testdf['41']+1)
    testdf = testdf.round(10)
    columns = fdf.columns.tolist()
    columns.remove('0')
    columns.remove('1')
    columns.remove('2')
    # columns.remove('19')
    # columns.remove('43')
    # columns.remove('45')
    # columns.remove('54')
    # columns.remove('55')
    columns.remove('63')
    # columns.remove('69')

    s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
    # s_file = '/Users/ufenqi/Downloads/featurename_update'
    feaidlist = []
    feanamelist = []
    # feaidlist.append('0')
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
    columns = feaidlist
    # print columns
    columns.remove('2')
    # columns.remove('19')
    # columns.remove('43')
    # columns.remove('45')
    # columns.remove('54')
    # columns.remove('55')
    columns.remove('63')

    quancol = {}
    dflen = len(fdf)
    tiandic = {}
    wfp = open(out_file, 'w')
    for col in columns:
        print col
        vvl = []
        vvr = []
        vll = list(set(fdf[col].values.tolist()))
        for vl in vll:
            fmin = fdf[col].quantile(0.0001)
            fmax = fdf[col].quantile(0.9999)
            if vl < fmin:
                vvl.append(vl)
                vvr.append(fmin)
            if vl > fmax:
                vvl.append(vl)
                vvr.append(fmax)
        if len(vvl) != 0:
            fdf[col].replace(vvl, vvr, inplace=True)
        if len(fdf[col].unique()) < 2:
            del fdf[col]
            del testdf[col]
            continue
        else:
            tiandic[col] = [round(fmin, 10), round(fmax, 10)]
            # wfp.write(str(col) + '\t' + str(fdf[col].quantile(0.0001)) + '\t' + str(fdf[col].quantile(0.9999)) + '\n')
        print testdf[col]
        testvll = list(set(testdf[col].values.tolist()))
        vvl = []
        vvr = []
        for tvl in testvll:
            # fmin=fdf[col].quantile(0.0001)
            # fmax=fdf[col].quantile(0.9999)
            if tvl < fmin:
                vvl.append(tvl)
                vvr.append(fmin)
            if tvl > fmax:
                vvl.append(tvl)
                vvr.append(fmax)
        if len(vvl) != 0:
            testdf[col].replace(vvl, vvr, inplace=True)
            # colmaxlen = fdf[col].value_counts().values.tolist()[0]
            # if colmaxlen>(dflen*0.8):
            #     del fdf[col]
    json.dump(tiandic, wfp)
    # fdf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfdelinner.csv',index=False)
    testdf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeaturedel_501.csv', index=False)
if __name__ == '__main__':
    # train()
    test()
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


