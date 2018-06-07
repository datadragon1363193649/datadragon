# -*- cocoding: utf-8 -*-
from sklearn.pipeline import Pipeline
# from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
import pandas as pd
from sklearn import cross_validation, metrics
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import seaborn as sns
from sklearn.metrics import roc_curve
from sklearn.metrics import auc
import sys
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
# 筛选没有命中制定规则的用户
def rule_refuse():
    tdf=pd.read_csv('/Users/ufenqi/Documents/rule814.csv')
    s_file='/Users/ufenqi/Documents/dataming/base1/rule/rule_class'
    s2_file='/Users/ufenqi/Documents/score814.csv'
    s3_file='/Users/ufenqi/Downloads/uidlist'
    uidlist=[]
    with open(s3_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            uidlist.append(int(linelist[0]))
    # print tdf.dtypes
    print len(tdf['userId'].value_counts())
    tdf=tdf[tdf['userId'].isin(uidlist)]
    print len(tdf)
    print len(tdf['userId'].value_counts())
    print len(uidlist)
    print tdf['userId'].value_counts()
    # ul=['客户与紧急联系人联系次数','银行卡消费情况异常月份','通讯录个数']
    ul=['内部欺诈得分直拒']
    uidlist=[]
    with open(s2_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            uidlist.append(linelist[0])
    print len(uidlist)
    out_file='/Users/ufenqi/Documents/ruleid_new814'
    wfp=open(out_file,'w')
    refuselist=[]
    with open(s_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            # print linelist[0]
            if linelist[1]=='预警':
                continue
            elif linelist[0] in ul:
                continue
            else:
                refuselist.append(linelist[0])
    rowall=tdf['userId'].values.tolist()
    t2df = tdf[tdf['ruleName'].isin(ul)]
    print 111,len(t2df)
    print len(t2df)
    rowlist=t2df['userId'].values.tolist()
    print rowlist
    t1df=tdf[tdf['userId'].isin(rowlist)]
    # cnamelist=['闪银通讯记录','手机身份信息匹配','命中手机归属直拒城市','同盾','命中身份证归属直拒城市','百融黑名单拒绝','命中直拒行业',
    #            '短时间IP地址流量异常','中智诚黑名单拒绝','LBS短时间内某个区域自然流量异常规则1','LBS短时间内某个区域自然流量异常规则2']
    print len(t1df)
    for cn in refuselist:
        rowlist=t1df['userId'][t1df['ruleName']==cn].values.tolist()
        print rowlist
        for r in rowlist:
            vl=t2df['userId'].values.tolist()
            if r in vl:
                # print 'qqq'
                t2df = t2df.drop(t2df[t2df['userId']==r].index)
        print len(t2df)
    rowlist = t2df['userId'].values.tolist()
    print len(rowlist)
    realvaluelist = t2df['realValue'].values.tolist()
    for i in range(0, len(rowlist)):
        if str(rowlist[i]) in uidlist:
            uidlist.remove(str(rowlist[i]))
        # if realvaluelist[i]>20:
        #     continue
            # print len(uidlist)
        wfp.write(str(rowlist[i]) + "," + str(realvaluelist[i]) + '\n')
# 预测值分布绘图
def rule_analyze():
    s_file='/Users/ufenqi/Documents/rulescorepass_814'
    # s_file = '/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/twomodel'
    userlist = []
    with open(s_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[1] == 'NULL' or linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
                continue
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['uid','phn', 'wecash', 'rulenum', 'yuqiday', 'sfscore1', 'sfscore2'])
    aucdf['sfscore2'] = aucdf['sfscore2'].astype('float')
    # aucdf['rulescore'] = aucdf['rulescore'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf = aucdf.round(1)
    print len(aucdf)
    aucdf = aucdf[aucdf['yuqiday'] > -3]
    print len(aucdf)
    # aucdf=aucdf[aucdf['rulescore']<20]

    y_predprobl=aucdf['sfscore2'].values.tolist()
    print len(y_predprobl)
    print aucdf['sfscore2'].value_counts()
    plt.figure(1)
    n, bins, patches = plt.hist(y_predprobl, 100, normed=1, facecolor='g', alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()
    plt.figure(1)
    plt.plot([0, 1], [0, 1], 'k--')


if __name__ == '__main__':
    rule_refuse()
    rule_analyze()



# awk -F',' 'NR==FNR {a[$1]=$2} NR>FNR&& ($1 in a) {print $0","a[$1]>"rulescorerefuse"}' /Users/ufenqi/Documents/ruleid  /Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/twomodel