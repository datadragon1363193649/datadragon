# -*- encoding: utf-8 -*-
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
if __name__ == '__main__':
    feature_value()

