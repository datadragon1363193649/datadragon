# -*- encoding: utf-8 -*-
'''
绘制每天用户每个特征的变化曲线图
'''
import os
import sys
# import pandas as pda
import sys
import traceback
import json
import pymongo as pm
import time
import datetime
import offline_db_confdate as dconf
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
reload(sys)
sys.setdefaultencoding('utf-8')
featuredic={}
def plot_feature(filename):
    s1_file='/Users/ufenqi/Documents/dataming/base1/modelmonitor/basefeature/'+filename+'.csv'
    namelist=[]
    with open(s1_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            if linelist[0]=='featurename':
                continue
            featuredic.setdefault(linelist[0],[])
            featuredic[linelist[0]].append(float(linelist[2]))
            namelist.append(linelist[0])
    return namelist
def feature_value():
    datelist = ['2017-08-01','2017-08-02','2017-08-03','2017-08-04','2017-08-05',
                '2017-08-06','2017-08-07','2017-08-08','2017-08-09','2017-08-10',
                '2017-08-11','2017-08-12', '2017-08-13', '2017-08-14', '2017-08-15',
                '2017-08-16', '2017-08-17', '2017-08-18','2017-08-19', '2017-08-20']
    namelist=[]
    for dl in datelist:
        namelist=plot_feature(dl)
    # froup_labels=['2017-08-01','2017-08-02','2017-08-03','2017-08-04','2017-08-05',
    #             '2017-08-06','2017-08-07','2017-08-08','2017-08-09','2017-08-10',
    #             '2017-08-11','2017-08-12', '2017-08-13', '2017-08-14', '2017-08-15',
    #             '2017-08-16', '2017-08-17', '2017-08-18','2017-08-19', '2017-08-20']
    for nl in namelist:

        a=featuredic[nl]
        adf=pd.DataFrame(a)
        adf.index=datelist
        print adf[0]
        fig=plt.figure()
        ax1=fig.add_subplot(1,1,1)
        ax1.set_xlabel(nl)
        # ax1.xticks(froup_labels,)
        adf[0].plot(kind='line')
        plt.show()
def feature_dis():
    tdf10=pd.read_csv('/Users/ufenqi/Documents/dataming/call_c/dataset/codea7_feature_10.csv')
    tdf11 = pd.read_csv('/Users/ufenqi/Documents/dataming/call_c/dataset/codea7_feature_11.csv')
    tdf4 = pd.read_csv('/Users/ufenqi/Documents/dataming/call_c/dataset/codea7_feature_4.csv')

    # print tdf10.describe().T
    des_df10=tdf10.describe().T
    des_df11 = tdf11.describe().T
    des_df4 = tdf4.describe().T
    des_df10=des_df10['mean']
    des_df11 = des_df11['mean']
    des_df4 = des_df4['mean']
    # plot_feature(des_df10)
    print des_df10['2']
    featuredic = {}
    for ind in des_df10.index:
        featuredic.setdefault(ind, [])
        featuredic[ind].append(des_df10[ind])
        featuredic[ind].append(des_df11[ind])
        featuredic[ind].append(des_df4[ind])
    print featuredic
    datelist=['10','11','4']
    ci=1
    plt.subplots_adjust(left=0.1, bottom=0.1, right=0.9, top=1, hspace=1, wspace=1)

    for nl in des_df10.index:
        # plt.figure(ci)
        # plt.ylim(1, 6)
        a = featuredic[nl]
        adf = pd.DataFrame(a)
        adf.index = datelist
        print adf[0]
        # plt.figure(figsize=(8,8),dpi=80)
        ax1 = plt.subplot(10, 5, ci)
        ci+=1
        # if ci>10:
        #     break
        ax1.set_xlabel(nl)
        # ax1.xticks(froup_labels,)
        adf[0].plot(kind='line')
    plt.show()
    # print des_df10.index
    # print des_df10['mean']
def weight():
    weightjson='/Users/ufenqi/Documents/dataming/call_c/parameter/featurenameAndWeight.json'
    name_file = '/Users/ufenqi/Documents/dataming/call_c/parameter/featurename_online'
    name_dic={}
    with open(name_file) as fp:
        for line in fp:
            linelist=line.strip().split(',')
            name_dic[linelist[1]]=linelist[0]
    with open(weightjson) as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    f = zip(cc.values(), cc.keys())
    f=sorted(f)
    f=list(f)
    # print type(f)
    for fi in f:
        # print fi[0]
        if fi[1] in name_dic:
            print str(name_dic[fi[1]])+","+str(fi[0])
if __name__ == '__main__':
    # feature_value()
    # feature_dis()
    weight()