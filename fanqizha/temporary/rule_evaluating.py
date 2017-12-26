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
def rulerefuse():
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
    # print tdf['ruleName'].value_counts()
    rowall=tdf['userId'].values.tolist()
    # t2df=tdf[tdf['ruleName'].isin(['通讯录个数'])]
    t2df = tdf[tdf['ruleName'].isin(ul)]
    print 111,len(t2df)
    print len(t2df)
    # rowlist=tdf['userId'][tdf['ruleName']=='客户与紧急联系人联系次数'].values.tolist()
    rowlist=t2df['userId'].values.tolist()
    print rowlist
    # print t2df.head()
    #
    # print len(rowlist)
    # print len(uidlist)
    # for uid in uidlist:
    #     wfp.write(str(uid)+",10"+'\n')
    # print tdf[tdf['ruleName']=='京东白条信用分直拒'].head(100)
    # print tdf['ruleName'][tdf['ruleName']=='客户通讯录通话详单申请客户个数异常'].value_counts()
    # print rowlist.head()
    # print len(rowlist)
    t1df=tdf[tdf['userId'].isin(rowlist)]
    # print t1df.head()
    # t1df.to_csv('/Users/ufenqi/Documents/rule511_1.csv',index=False)
    # t1df.to_excel('/Users/ufenqi/Documents/rule511_1.xlsx',index=False)
    # cnamelist=['闪银通讯记录','手机身份信息匹配','命中手机归属直拒城市','同盾','命中身份证归属直拒城市','百融黑名单拒绝','命中直拒行业',
    #            '短时间IP地址流量异常','中智诚黑名单拒绝','LBS短时间内某个区域自然流量异常规则1','LBS短时间内某个区域自然流量异常规则2']
    # print refuselist[0]
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
        # t1df=t1df.drop(t1df[t1df['userId'].isin(rowlist)].index)
    # print t1df['ruleName'].value_counts()
    # print t1df.head()
    # print len(t1df[t1df['ruleName'].isin(ul)])
    rowlist = t2df['userId'].values.tolist()
    print len(rowlist)
    realvaluelist = t2df['realValue'].values.tolist()
    for i in range(0, len(rowlist)):
        # print rowlist[i]
        if str(rowlist[i]) in uidlist:
            uidlist.remove(str(rowlist[i]))
        # if realvaluelist[i]>20:
        #     continue
            # print len(uidlist)
        wfp.write(str(rowlist[i]) + "," + str(realvaluelist[i]) + '\n')
    # t1df.to_excel('/Users/ufenqi/Documents/rule511_1.xlsx',index=False)
    #
    # rowlist=t1df['userId'][t1df['ruleName']=='手机身份信息匹配'].values.tolist()
    # print len(rowlist)
    # t1df=t1df.drop(t1df[t1df['userId'].isin(rowlist)].index)
    # print t1df['ruleName'].value_counts()
def ruleanalyze():
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
def merge():
    s1_file='/Users/ufenqi/Documents/ruleid_con814'
    s2_file='/Users/ufenqi/Documents/ruleid_imp814'
    s3_file='/Users/ufenqi/Documents/ruleid_all814'
    out_file = '/Users/ufenqi/Documents/ruleid_pass_814'
    wfp = open(out_file, 'w')
    idall={}
    idrefuse={}
    with open(s1_file,'r') as fp:
        m = 0
        n=0
        for line in fp:
            n+=1
            linelist=line.strip().split(',')
            idall[linelist[0]]=0
            if float(linelist[1])<20:
                m+=1
                idrefuse[linelist[0]]=0
        print 111,n,m
    with open(s2_file,'r') as fp:
        m=0
        n=0
        for line in fp:
            n+=1
            linelist=line.strip().split(',')
            idall[linelist[0]]=0
            if float(linelist[1])<2:
                m+=1
                idrefuse[linelist[0]]=0
    print n,m
    with open(s3_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            idall[linelist[0]]=0
    idlist=idall.keys()
    print len(idlist)
    for ar in idrefuse:
        if ar in idlist:
            idlist.remove(ar)
    print len(idlist)
    for i in idlist:
        wfp.write(i+",0"+"\n")
def phnmarge():
    # s1file='/Users/ufenqi/Documents/dataming/base1/rule/phnpass'
    s2file='/Users/ufenqi/Documents/dataming/base1/rule/ruleid_pass_814'
    s3file='/Users/ufenqi/Downloads/query_result.csv'
    outfile='/Users/ufenqi/Documents/dataming/base1/rule/phnallpass'
    wfp=open(outfile,'w')
    tdf=pd.read_csv(s3file)
    uidlist=tdf['用户ID'].values.tolist()
    uphnlist=tdf['电话'].values.tolist()
    phnlist = tdf['电话'][tdf['是否通过认证(是:0 否:1)'] == 0].values.tolist()
    print len(phnlist)
    with open(s2file,'r') as fp:
        a=[]
        for line in fp:
            linelist=line.strip().split(',')
            if int(linelist[0]) in uidlist:
                phn=uphnlist[uidlist.index(int(linelist[0]))]
                if phn in phnlist:
                    print phn
                    continue
                phnlist.append(phn)
                a.append(linelist[0])
            else:
                a.append(linelist[0])
    print len(a)
    for ph in phnlist:
        wfp.write(str(ph)+'\n')
def merge_feature():
    s1file='/Users/ufenqi/Documents/dataming/taobao/data/taobao_user'
    s2file='/Users/ufenqi/Documents/dataming/model_yu/data/scorecodenordelall_train_fea_first.csv'
    s3file = '/Users/ufenqi/Documents/dataming/taobao/data/taobao_user1'
    stfile = '/Users/ufenqi/Documents/dataming/model_yu/data/train_fitst_phn_target'
    outfile='/Users/ufenqi/Documents/dataming/taobao/data/taobao_user_train'
    wfp=open(outfile,'w')
    # stdic={}
    # with open(stfile,'r') as fp:
    #     for line in fp:
    #         linelist=line.strip().split(',')
    #         stdic[linelist[0]]=linelist[1]
    s1dic = {}
    with open(s1file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            print linelist[-1]
            if float(linelist[-1])>6:
                s1dic[linelist[0]] = '0'
            else:
                s1dic[linelist[0]] = '1'
            # print len(linelist[1:])
    # s2dic = {}
    # with open(s2file, 'r') as fp:
    #     for line in fp:
    #         linelist = line.strip().split(',')
    #         s2dic[linelist[0]] = linelist[1:]
    with open(s3file,'r') as fp:
        for line in fp:
            fl=[]
            linelist=line.strip().split('\t')
            if linelist[0] in s1dic:
                fl=[linelist[0],s1dic[linelist[0]]]
                # fl=fl+s1dic[linelist[0]]
                # fl=fl +s1dic[linelist[0]]
                fl=fl+linelist[1:]
                # print len(s2dic[linelist[0]]),len(linelist[1:])
                # fl=linelist+s2dic[linelist[0]]
                print linelist[0],len(fl)
                wfp.write('\t'.join(fl)+'\n')
def screen_feature():
    s1file='/Users/ufenqi/Documents/dataming/base2/overdue/score_td_7152'
    s2file='/Users/ufenqi/Documents/dataming/base2/overdue/g_phn_7_15'
    s3file='/Users/ufenqi/Documents/dataming/base2/overdue/y_phn_7_15'
    outfile='/Users/ufenqi/Documents/dataming/base2/overdue/score_td_715_screen'
    wfp=open(outfile,'w')
    phndic={}
    with open(s3file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            phndic[linelist[0]] = 0
    with open(s2file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            phndic[linelist[0]] = 0

    with open(s1file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            if linelist[0] in phndic:
                wfp.write(line.strip()+'\n')



if __name__ == '__main__':
    # rulerefuse()
    # ruleanalyze()
    # merge()
    # phnmarge()
    merge_feature()
    # screen_feature()



# awk -F',' 'NR==FNR {a[$1]=$2} NR>FNR&& ($1 in a) {print $0","a[$1]>"rulescorerefuse"}' /Users/ufenqi/Documents/ruleid  /Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/twomodel