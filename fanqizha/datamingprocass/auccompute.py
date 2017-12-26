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
from scipy.stats import ks_2samp
def auctaobao():
    f_file = '/Users/ufenqi/Documents/dataming/taobao/data/score_taobao1_3'

    ffdic ={}
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            ffdic.setdefault(linelist[0],linelist[-1])
    userlist = []
    with open(f_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[1] == 'NULL' or linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            # linelist.remove(linelist[0])
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,columns=['phn', 'target', 'tscore'])
    # print aucdf.head()
    aucdf['target'] = aucdf['target'].astype('float')
    aucdf['tscore'] = aucdf['tscore'].astype('float')
    print aucdf[aucdf['tscore']>473].__len__()
    # aucdf = aucdf[aucdf['yuqiday1'] > -1]
    print len(aucdf)
    print aucdf['target'].value_counts()

    print '************************************************'
    # print aucdf['yuqiday1'].value_counts()
    # print "wecash AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['wecash'])
    # print "sfscore1 AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['target'], aucdf['tscore'])
    # print "sfscore2 AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['sfscore2'])
    get_ks = lambda y_pred, y_true: ks_2samp(y_pred[y_true == 1], y_pred[y_true != 1]).statistic
    print 'ks值', get_ks(aucdf['tscore'], aucdf['target'])
    aucdfw = aucdf.sort_values(by='tscore', ascending=False)


    # zhenglist = []
    # fulist = []
    # calist = []
    # xlist = []
    # zhengz = len(aucdfw[aucdfw['target'] == 1])
    # fuz = len(aucdfw[aucdfw['target'] == 0])
    # for i in range(0,100):
    #     lnum=int(len(aucdf)*i/100)
    #     print lnum
    #     adf=aucdfw.head(lnum)
    #     z=len(adf[adf['target']==1])*1.0/zhengz
    #     f=len(adf[adf['target'] == 0])*1.0/fuz
    #     zhenglist.append(z)
    #     fulist.append(f)
    #     calist.append(abs(z-f))
    #     print i,abs(z-f)
    #     xlist.append(i)
    #
    aucdf1 = aucdf[aucdf['target'] ==0]
    # aucdf = aucdf[aucdf['yuqiday1'] > -1]
    print 'len', aucdf.__len__()
    fen1 = 0.47
    print fen1
    adf1 = aucdf1[aucdf1['tscore'] > fen1]
    adf1_2 = aucdf1[aucdf1['tscore'] <= fen1]
    print '真实坏用户'
    print 'true', len(adf1)
    print 'false', len(adf1_2)
    aucdf2 = aucdf[aucdf['target'] == 1]
    # aucdf = aucdf[aucdf['yuqiday1'] > -1]
    print 'len', aucdf.__len__()
    adf1 = aucdf2[aucdf2['tscore'] > fen1]
    adf1_2 = aucdf2[aucdf2['tscore'] <= fen1]
    print '真实好用户'
    print 'true', len(adf1)
    print 'false', len(adf1_2)

    # fig, ax = plt.subplots()
    # plt.plot(xlist, zhenglist, 'r', linewidth=2)
    # plt.plot(xlist, fulist, 'g', linewidth=2)
    # plt.plot(xlist, calist, 'b', linewidth=2)
    # plt.ylim(ymin=0)
    # plt.show()
    plt.figure(1)
    y_predprobl = aucdf['tscore'].values.tolist()
    for i in range(0,10):
        num=0
        for ui in range(0,len(y_predprobl)):
            # index1 = int(0.1 * i * len(y_predprobl))
            if y_predprobl[ui]>0.1 * i:
                num+=1
        print 'scor:',i*0.1,'acc:',1.0*num/len(y_predprobl)
    num=0
    for ui in range(0,len(y_predprobl)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if y_predprobl[ui]>0.49:
            num+=1
    print 'scor:',0.49,'acc:',1.0*num/len(y_predprobl)
    # n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()


def aucmyself():
    ypre = []
    ytrue = []
    ywecash=[]
    int_file = '/Users/ufenqi/Documents/dataming/model_yu/data/score_m1_825'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[1] == 'NULL' or linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            # linelist.remove(linelist[0])
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['phn', 'time', 'sfscore1','yuqiday','sfscore2'])

    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['sfscore2'] = aucdf['sfscore2'].astype('float')
    # aucdf['sfscore3'] = aucdf['sfscore3'].astype('float')
    aucdf['sfscore1'] = aucdf['sfscore1'].astype('float')
    # aucdf['wecash'] = aucdf['wecash'].astype('float')
    # print aucdf.describe()
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    # print len(aucdf)
    aucdf['yuqiday1'] = aucdf['yuqiday']
    aucdf = aucdf[aucdf['yuqiday1']> 29]
    aucdf = aucdf[aucdf['yuqiday1'] >-1]
    print 'len',aucdf.__len__()
    fen1=aucdf['sfscore1'].quantile(0.45)
    fen2=aucdf['sfscore2'].quantile(0.40)
    # print fen2
    fen1=0.75
    fen2=0.21
    print fen1
    print fen2
    adf1=aucdf[aucdf['sfscore1']>fen1]
    adf1_2 = aucdf[aucdf['sfscore1'] <= fen1]
    adf2 = aucdf[aucdf['sfscore2'] > fen2]
    # print len(adf2)*1.0/len(aucdf)
    # print adf2.describe()
    adf2_2=aucdf[aucdf['sfscore2'] <=fen2]
    print 'true', len(adf1)
    print 'true', len(adf2)
    print 'false',len(adf1_2)
    print 'false',len(adf2_2)
    n11=len(adf1[adf1['yuqiday1']<30])
    n12 = len(adf1_2[adf1_2['yuqiday1'] >= 30])
    n21=len(adf2[adf2['yuqiday1']<30])
    n22=len(adf2_2[adf2_2['yuqiday1']>=30])
    numo=n21+n22
    print  'model1 acc', (n11+n12) * 1.0 / len(aucdf)
    print  'model2 acc',numo*1.0/len(aucdf)
    y_predprobl = aucdf['sfscore1'].values.tolist()
    # plt.figure(1)
    # n, bins, patches = plt.hist(y_predprobl, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')
    # y_predprobl = aucdf['sfscore2'].values.tolist()
    # plt.figure(1)
    # n, bins, patches = plt.hist(y_predprobl, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')



    # aucdfw = aucdf.sort_values(by='sfscore1', ascending=False)
    # aucdfm = aucdf.sort_values(by='sfscore2', ascending=False)
    # passnum=int((len(aucdfw) * 6) / 10)
    # # aucpassw1 = aucdfw.head(int((len(aucdfw) * 6) / 10))
    # # aucpassm1 = aucdfm.drop(aucpassw1.index)
    # aucdfw1=aucdfw[aucdfw['sfscore1']>]
    # with open(f2_file, 'r') as fp:
    #     zheng = 0
    #     fu = 0
    #     prezheng = 0
    #     prefu = 0
    #     for line in fp:
    #         linelist = line.strip().split(',')
    #         if linelist[2]=='' or linelist[-1]=='':
    #             continue
    #         print linelist
    #         if float(linelist[-1])<0.75:
    #             if float(linelist[1]) > 29:
    #                 prefu+=1
    #             else:
    #                 prezheng+=1
    #         ypre.append(float(linelist[-1]))
    #
    #         if float(linelist[1])>29:
    #             ytrue.append(1)
    #             fu+=1
    #         else:
    #             ytrue.append(0)
    #             zheng+=1
    #         ywecash.append(float(linelist[2]))
    # print "Accuracy : %.4g" % metrics.accuracy_score(ytrue, ypre)
    # print "AUC Score (Train): %f" % metrics.roc_auc_score(ytrue, ypre)
    # print len(ypre)
    # print len(ytrue)
    # print len(ywecash)
    # print 'zheng',zheng
    # print 'fu',fu
    # print 'prezheng', prezheng
    # print '[refu', prefu
    #
    # fpr, tpr, thresholds = roc_curve(ytrue, ypre, pos_label=0)
    # print 'myself',auc(fpr, tpr)
    # fpr, tpr, thresholds = roc_curve(ytrue, ywecash, pos_label=0)
    # print 'wecash', auc(fpr, tpr)
def quchongmax():
    input_file1='/Users/ufenqi/Downloads/auctranstion/auct1'
    input_file2 = '/Users/ufenqi/Downloads/auctranstion/useridmax'
    out_file='/Users/ufenqi/Downloads/auctranstion/auctqu'
    wpt=open(out_file,'w')
    indi={}
    with open(input_file2, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            indi[linelist[0]]=linelist[1]
            # print indi[linelist[0]]
    auqqu={}
    with open(input_file1, 'r') as fp1:
        for line in fp1:
            linelist = line.strip().split(',')
            if linelist[5]=='-1' or linelist[5]=='-2':
                continue
            if linelist[0] in indi:
                print indi[linelist[0]],linelist[5]
                if linelist[5] ==indi[linelist[0]]:
                    auqqu.setdefault(linelist[0],linelist)
                    # print linelist[5]
                    # print linelist
    for dic in auqqu:
        wpt.write(','.join(auqqu[dic]) + '\n')
                # else:
                    # print 'error',linelist
def auctransition():
    int_file = '/Users/ufenqi/Downloads/query_result.csv'
    out_file='/Users/ufenqi/Downloads/uidlist'
    wfp=open(out_file,'w')
    userlist=[]
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[-3]=='NULL' or linelist[-4]=='NULL'or linelist[-2]=='NULL':
                continue
            userlist.append(linelist)
            # print len(linelist)
    aucdf=pd.DataFrame(userlist,columns=['userid','phn','time','passtf','wecash','myself','yuqiday','rulenum'])
    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['myself'] = aucdf['myself'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1']=aucdf['yuqiday']
    print aucdf.__len__()

    # aucdf=aucdf[aucdf['passtf']=='0']
    aucdf = aucdf[(aucdf['myself'] > 0.73) & (aucdf['myself'] <= 0.75)]
    uidlist=aucdf['userid'].values.tolist()
    for i in uidlist:
        wfp.write(i+'\n')
    # print aucdf['myself'].head(100)
    print aucdf.__len__()
    # aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    # aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    # fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    # print 'wecash', auc(fpr, tpr)
    # yt=aucdf['yuqiday1'].values.tolist()
    wel=aucdf['wecash'].values.tolist()
    y_predprobl = aucdf['myself'].values.tolist()
    # for i in range(0,10):
    #     num=0
    #     for ui in range(0,len(aucdf)):
    #         # index1 = int(0.1 * i * len(y_predprobl))
    #         if y_predprobl[ui]>0.1 * i:
    #             num+=1
    #     print 'scor:',i*0.1,'pass:',1.0*num/len(aucdf)
    num = 0
    for ui in range(0,len(aucdf)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if y_predprobl[ui]>0.1 * 7.3:
            num+=1
    print 'scor:',7.3,'pass:',1.0*num/len(aucdf)
    num = 0
    for ui in range(0,len(aucdf)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if y_predprobl[ui]>0.1 * 7.5:
            num+=1
    print 'scor:',7.5,'pass:',1.0*num/len(aucdf)
    # wel.sort()
    # for i in range(0, 10):
    #     num = 0
    #     for ui in range(0, len(yt)):
    #         index1 = int(0.1*i * len(yt))
    #         if wel[ui] > wel[index1]:
    #             if yt[ui] == 1:
    #                 num += 1
    #         else:
    #             if yt[ui] == 0:
    #                 num += 1
    #     print 'biao:', i * 0.1, 'acc:', 1.0 * num / len(yt)
    # # wel.sort()
    # index1 = int(0.4 * len(yt))
    # print wel[index1]
    # plt.figure(2)
    # n, bins, patches = plt.hist(wel, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()

    # aucdfgbdt=pd.DataFrame(userlist, columns=['userid', 'time', 'passtf', 'wecash', 'myself', 'yuqiday', 'rulenum', 'gbdt'])
    # aucdfgbdt['yuqiday'] = aucdfgbdt['yuqiday'].astype('float')
    # # aucdf['myself'] = aucdf['myself'].astype('float')
    # aucdfgbdt['wecash'] = aucdfgbdt['wecash'].astype('float')
    # aucdfgbdt['gbdt'] = aucdfgbdt['gbdt'].astype('float')
    # aucdfgbdt['yuqiday1'] = aucdfgbdt['yuqiday']
    #
    # aucdfgbdt['yuqiday1'][(aucdfgbdt['yuqiday1'] > -1) & (aucdfgbdt['yuqiday1'] < 30)] = 0
    # aucdfgbdt['yuqiday1'][aucdfgbdt['yuqiday1'] != 0] = 1
    # # print aucdfgbdt['yuqiday1'].value_counts()
    # fpr, tpr, thresholds = roc_curve(aucdfgbdt['yuqiday1'], aucdfgbdt['gbdt'], pos_label=1)
    # # print 'gbdt', auc(fpr, tpr)
    # a=[2,4,6,8]
    # aucdfw=aucdf.sort_values(by='wecash',ascending=False)
    # aucdfm = aucdf.sort_values(by='gbdt')
    # for i in a:
    #     aucpassw = aucdfw.head(int((len(aucdfw) * i) / 10))
    #     print 'wecashpass', i
    #     print aucpassw['yuqiday1'].value_counts()
    #     print len(aucpassw)
    #     wecashzhi = (len(aucpassw[aucpassw['yuqiday1'] != 1]) * 1.0) / len(aucpassw)
    #     print 'wecashyuqi', wecashzhi
    #     print '+++++++++++++++++'
    #     aucpassm = aucdfm.head(int((len(aucdfm) * i) / 10))
    #     print 'myselfpass', i
    #     print aucpassm['yuqiday1'].value_counts()
    #     print len(aucpassm)
    #     myselfz = (len(aucpassm[aucpassm['yuqiday1'] != 1]) * 1.0) / len(aucpassm)
    #     print 'myselfyuqi', myselfz
    #     print '|||||||||||||||||||||'
    #     print 'wecash-myself', wecashzhi - myselfz
    #     print '|||||||||||||||||||||'
    # print '************************************************'
def auctransition3():
    int_file = '/Users/ufenqi/Downloads/query_result.csv'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
                continue
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['userid', 'phn','time','ispass','wecash', 'sfscore','yuqiday','rulenum'])
    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['sfscore'] = aucdf['sfscore'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']
    aucdf=aucdf[aucdf['yuqiday1']>-1]
    # int_file = '/Users/ufenqi/Downloads/auctranstion/auctqu'
    # linlist=[]
    # with open(int_file, 'r') as fp2:
    #     for line in fp2:
    #         linelist = line.strip().split(',')
    #         if linelist[3]=='' or linelist[4]==''or linelist[-2]=='':
    #             continue
    #         linlist.append(linelist)
    # aucdf=pd.DataFrame(linlist,columns=['userid','time','passtf','wecash','myself','yuqiday','rulenum','gbdt'])
    #
    # # print aucdf.head()
    # aucdf['yuqiday'] = aucdf['yuqiday'].astype('int')
    # aucdf['myself'] = aucdf['myself'].astype('float')
    # aucdf['wecash'] = aucdf['wecash'].astype('float')
    aucdf['rulenum'] = aucdf['rulenum'].astype('float')
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    # aucdf['yuqiday1']=aucdf['yuqiday']

    # aucdfgbdt = aucdf
    print aucdf['rulenum'].value_counts()
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 3)] =1
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    print '************************************************'
    print aucdf['yuqiday1'].value_counts()
    fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    print 'wecash', auc(fpr, tpr)
    # print aucdfgbdt['yuqiday1'].value_counts()
    # aucdfgbdt['yuqiday1'][(aucdfgbdt['yuqiday1'] > -1) & (aucdfgbdt['yuqiday1'] < 30)] = 0
    # aucdfgbdt['yuqiday1'][aucdfgbdt['rulenum'] < 3] = 0
    # aucdfgbdt['yuqiday1'][aucdfgbdt['yuqiday1'] != 0] = 1
    fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['sfscore'], pos_label=1)
    print 'myself', auc(fpr, tpr)
    # a=[2,4,6,8]
    # aucdfw=aucdf.sort_values(by='wecash',ascending=False)
    # aucdfm = aucdf.sort_values(by='gbdt',ascending=False)
    # for i in a:
    #     aucpassw=aucdfw.head(int((len(aucdfw)*i)/10))
    #     print 'wecashpass',i
    #     print aucpassw['yuqiday1'].value_counts()
    #     print len(aucpassw)
    #     wecashzhi=(len(aucpassw[aucpassw['yuqiday1'] != 1])*1.0)/len(aucpassw)
    #     print 'wecashyuqi',wecashzhi
    #     print '+++++++++++++++++'
    #     aucpassm = aucdfm.head(int((len(aucdfm) * i) / 10))
    #     print 'myselfpass', i
    #     print aucpassm['yuqiday1'].value_counts()
    #     print len(aucpassm)
    #     myselfz=(len(aucpassm[aucpassm['yuqiday1'] != 1]) * 1.0) / len(aucpassm)
    #     print 'myselfyuqi', myselfz
    #     print '|||||||||||||||||||||'
    #     print 'wecash-myself',wecashzhi-myselfz
    #     print '|||||||||||||||||||||'
    # print '************************************************'
    # for i in a:
    #     aucpassm = aucdfm.head(int((len(aucdfm) * i) / 10))
    #     print 'pass', i
    #     print aucpassm['yuqiday1'].value_counts()
    #     print len(aucpassm)
    #     print 'yuqi', (len(aucpassm[aucpassm['yuqiday1'] != 1]) * 1.0) / len(aucpassm)
    #     print '+++++++++++++++++'
def aucscore():
    # int_file = '/Users/ufenqi/Documents/feayuqi501'
    # int_file = '/Users/ufenqi/Documents/dataming/base1/data/data_1510/test/feayuqi511'
    int_file='/Users/ufenqi/Documents/dataming/integration/data/score_bagging_911'
    # int_file = '/Users/ufenqi/Documents/dataming/base1/test/score_m1_715'
    # int_file='/Users/ufenqi/Documents/dataming/bank/data/score_bank_913'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[1] == 'NULL' or linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            # linelist.remove(linelist[0])
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['phn', 'time', 'sfscore1','yuqiday','sfscore2'])

    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['sfscore2'] = aucdf['sfscore2'].astype('float')
    # aucdf['sfscore3'] = aucdf['sfscore3'].astype('float')
    aucdf['sfscore1'] = aucdf['sfscore1'].astype('float')
    # aucdf['wecash'] = aucdf['wecash'].astype('float')
    # print aucdf.describe()
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']

    # aucdfw = aucdf.sort_values(by='wecash', ascending=False)
    # scorelist = aucdfw['wecash'].values.tolist()
    # scorel = scorelist[int(0.1 * 6 * len(scorelist))]
    # print  aucdf['yuqiday1'].value_counts()
    aucdf = aucdf[aucdf['yuqiday1'] > -1]
    print len(aucdf)
    # aucdf = aucdf[aucdf['yuqiday1'] <30]
    # print len(aucdf)
    a1=aucdf[['phn','yuqiday','sfscore2']]
    a1.to_csv('/Users/ufenqi/Documents/fea_score615.csv',index=False)
    # aucdf['rulenum'] = aucdf['rulenum'].astype('float')

    # print aucdf['rulenum'].value_counts()
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] <30)] = 1
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    print aucdf['yuqiday1'].value_counts()

    print '************************************************'
    # print aucdf['yuqiday1'].value_counts()
    # print "wecash AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['wecash'])
    print "sfscore1 AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['sfscore1'])
    print "sfscore2 AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['sfscore2'])
    # fpr_grd_lm, tpr_grd_lm, _ = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    fpr_grd_lm1, tpr_grd_lm1, _ = roc_curve(aucdf['yuqiday1'], aucdf['sfscore1'], pos_label=1)
    fpr_grd_lm2, tpr_grd_lm2, _ = roc_curve(aucdf['yuqiday1'], aucdf['sfscore2'], pos_label=1)
    aucdfw = aucdf.sort_values(by='sfscore2', ascending=False)
    yudaylist = aucdfw['yuqiday1'].values.tolist()
    # scorelist=aucdfw['wecash'].values.tolist()
    y_predprobl=aucdfw['sfscore2'].values.tolist()
    get_ks = lambda y_pred, y_true: ks_2samp(y_pred[y_true == 1], y_pred[y_true != 1]).statistic

    print 'ks值', get_ks(aucdfw['sfscore2'], aucdfw['yuqiday1'])
    zhenglist=[]
    fulist=[]
    calist=[]
    xlist=[]
    zhengz=len(aucdfw[aucdfw['yuqiday1']==1])
    fuz = len(aucdfw[aucdfw['yuqiday1'] == 0])
    for i in range(0,100):
        lnum=int(len(aucdfw)*i/100)
        print lnum
        adf=aucdfw.head(lnum)
        z=len(adf[adf['yuqiday1']==1])*1.0/zhengz
        f=len(adf[adf['yuqiday1'] == 0])*1.0/fuz
        zhenglist.append(z)
        fulist.append(f)
        calist.append(abs(z-f))
        print i,abs(z-f)
        xlist.append(i)
    for i in range(0,10):
        num=0
        for ui in range(0,len(aucdfw)):
            # index1 = int(0.1 * i * len(y_predprobl))
            if y_predprobl[ui]>(0.1 * i):
                num+=1
        print 'scor:',i*0.1,'pass:',1.0*num/len(aucdfw)
    num=0
    for ui in range(0,len(aucdfw)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if y_predprobl[ui]>( 0.21):
            num+=1
    print 'scor:',0.21,'pass:',1.0*num/len(aucdfw)
    red, blue = sns.color_palette("Set1", 2)
    print aucdf.head()
    sns.kdeplot(aucdf['sfscore2'][aucdf['yuqiday1'] == 1], shade=True, color=red)
    sns.kdeplot(aucdf['sfscore2'][aucdf['yuqiday1'] == 0], shade=True, color=blue)

    # aucdfm1 = aucdf.sort_values(by='sfscore1', ascending=False)
    # aucdfm2 = aucdf.sort_values(by='sfscore2', ascending=False)
    # for i in range(0,10):
    #     num=0
    #     for ui in range(0,len(scorelist)):
    #         index1 = int(0.1 * i * len(scorelist))
    #         if scorelist[ui]>(scorelist[index1]):
    #             if yudaylist[ui]==1:
    #                 num+=1
    #         else:
    #             if yudaylist[ui] == 0:
    #                 num+=1
    #     print 'pass:',i*0.1,'acc:',1.0*num/len(scorelist)
    #
    #
    # num=0
    # for ui in range(0,len(scorelist)):
    #     if scorelist[ui]>scorel:
    #         if yudaylist[ui]==1:
    #             num+=1
    #     else:
    #         if yudaylist[ui] == 0:
    #             num+=1
    # print scorel,'acc:',1.0*num/len(scorelist)
    # scorel = scorelist[int(0.1 * 6 * len(scorelist))]
    # prelist=[]
    # for ui in scorelist:
    #     if ui > scorel:
    #         prelist.append(1)
    #     else:
    #         prelist.append(0)
    # print metrics.confusion_matrix(yudaylist, prelist)
    # fig, ax = plt.subplots()
    # plt.plot(xlist, zhenglist, 'r', linewidth=2)
    # plt.plot(xlist, fulist, 'g', linewidth=2)
    # plt.plot(xlist, calist, 'b', linewidth=2)
    # plt.ylim(ymin=0)
    plt.show()
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')
    # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
    # plt.plot(fpr_grd_lm, tpr_grd_lm, label='wecash')
    # plt.plot(fpr_grd_lm1, tpr_grd_lm1, label='myself1')
    # plt.plot(fpr_grd_lm2, tpr_grd_lm2,label='myself2')
    # plt.xlabel('False positive rate')
    # plt.ylabel('True positive rate')
    # plt.legend(loc='best')
    # plt.show()
    # print aucdfw['sfscore2']
    # aucdfw['sfscore2']=np.log(aucdfw['sfscore2'])
    # print aucdfw['sfscore2']
    # y_predprobl = aucdfw['sfscore1'].values.tolist()
    # plt.figure(1)
    # n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')

    y_predprobl = aucdfw['sfscore2'].values.tolist()
    ylist=[]
    for y in y_predprobl:
        ylist.append(y)
    # plt.figure(1)
    # n, bins, patches = plt.hist(ylist, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('score')
    # plt.ylabel('proportion')
    # plt.title('model_score')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')

    # fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    # print 'wecash', auc(fpr, tpr)
    # fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf[''], pos_label=1)
    # print 'sfscore1', auc(fpr, tpr)
    # fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['sfscore2'], pos_label=1)
    # print 'sfscore2', auc(fpr, tpr)


def maxuserid():
    int_file = '/Users/ufenqi/Downloads/userinformaion'
    max_file = '/Users/ufenqi/Downloads/useridmax'
    output='/Users/ufenqi/Downloads/useridmaxinformation'
    lindic={}
    maxlist=[]
    wtp=open(output,'w')
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[3]=='' or linelist[4]=='':
                continue
            lindic.setdefault(linelist[0],linelist)
    with open(max_file, 'r') as fp1:
        for line in fp1:
            linelist = line.strip().split(',')
            if linelist[0] in lindic:
                lindic[linelist[0]][5]=linelist[1]
                # maxlist.append(lindic[linelist[0]])
                wtp.write(','.join(lindic[linelist[0]])+'\n')
    # print maxlist.__len__()
    # print maxlist[0]
def pro_fea_split(s, k):
    """
    蓄水池算法
    """
    re = s[:k]
    for i, e in enumerate(s[k:]):
        r = np.random.randint(0, k + i + 1)
        if r <= k - 1:
            re[r] = e
    return re
def split_date():
    pro_file = '/Users/ufenqi/Documents/cui_phn.csv'
    out_file='/Users/ufenqi/Documents/cui_phn1'
    datedic={}
    renzhenglist=[]
    phndic={}
    wfp=open(out_file,'w')
    with open(pro_file, 'r') as rfp:
        for line in rfp:
            # print line
            linelist = line.strip().split('\r')
            for ll in linelist:
                llist=ll.strip().split(',')
                phndic[llist[0]]=0
    for ph in phndic:
        wfp.write(ph+'\n')

            # print len(linelist),[linelist[0]]
            # for ll in linelist
    # print len(renzhenglist)
    # for i in datedic:
    #     print i,len(datedic[i])
    #     datelist=pro_fea_split(datedic[i],4000)
    #     renzhenglist+=datelist
    # print len(renzhenglist)
    # wtp=open(out_file,'w')
    # for ren in renzhenglist:
    #     wtp.write(','.join(ren)+'\n')
def wecashfenbu():
    int_file = '/Users/ufenqi/Downloads/testuserall1'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[3] == '' or linelist[4] == '' or linelist[-2] == '':
                continue
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['userid', 'time', 'passtf', 'wecash', 'myself', 'yuqiday', 'rulenum', 'gbdt'])
    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    # aucdf['myself'] = aucdf['myself'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']
    # aucdf = aucdf[aucdf['yuqiday1'] > -1]
    print aucdf.__len__()
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    print 'wecash', auc(fpr, tpr)
    n, bins, patches = plt.hist(aucdf['wecash'], 100, normed=1, facecolor='g', alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()

def guizenum():
    int_file = '/Users/ufenqi/Documents/dataming/base1/test/score_auc_710_a'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            userlist.append(linelist)
            # print len(linelist)
    # aucdf = pd.DataFrame(userlist,
    #                      columns=['userid', 'wecash', 'rulenum', 'yuqiday','myself1', 'myself2'])
    aucdf = pd.DataFrame(userlist,
                         columns=['phn', 'wecash', 'rulenum', 'sfscore1','yuqiday','sfscore2'])
    # print aucdf.head()
    aucdf['sfscore2'] = aucdf['sfscore2'].astype('float')
    aucdf['sfscore1'] = aucdf['sfscore1'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    aucdf['rulenum'] = aucdf['rulenum'].astype('float')
    # aucdf['myself2'] = aucdf['myself2'].astype('float')
    # aucdf['wecash'] = aucdf['wecash'].astype('float')
    # # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    # aucdf['yuqiday1'] = aucdf['yuqiday']
    # aucdf=aucdf[aucdf['yuqiday1']>-1]
    # print aucdf.__len__()
    # aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 3)] = 1
    # aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    # fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    # print 'wecash', auc(fpr, tpr)
    # fpr_grd_lm, tpr_grd_lm, _ = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    # fpr_grd_lm1, tpr_grd_lm1, _ = roc_curve(aucdf['yuqiday1'], aucdf['myself1'], pos_label=1)
    # print 'myself', auc(fpr_grd_lm1, tpr_grd_lm1)
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')
    # # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
    # plt.plot(fpr_grd_lm, tpr_grd_lm, label='wecash')
    # plt.plot(fpr_grd_lm1, tpr_grd_lm1, label='myself')
    # plt.xlabel('False positive rate')
    # plt.ylabel('True positive rate')
    # plt.legend(loc='best')
    # plt.show()
    # aucdfgbdt = pd.DataFrame(userlist,
    #                          columns=['userid', 'time', 'passtf', 'wecash', 'myself', 'yuqiday', 'rulenum', 'gbdt'])
    # aucdfgbdt['yuqiday'] = aucdfgbdt['yuqiday'].astype('float')
    # # aucdf['myself'] = aucdf['myself'].astype('float')
    # aucdfgbdt['wecash'] = aucdfgbdt['wecash'].astype('float')
    # aucdfgbdt['gbdt'] = aucdfgbdt['gbdt'].astype('float')
    # aucdfgbdt['yuqiday1'] = aucdfgbdt['yuqiday']
    #
    # aucdfgbdt['yuqiday1'][(aucdfgbdt['yuqiday1'] > -1) & (aucdfgbdt['yuqiday1'] < 30)] = 0
    # aucdfgbdt['yuqiday1'][aucdfgbdt['yuqiday1'] != 0] = 1
    # # print aucdfgbdt['yuqiday1'].value_counts()
    # fpr, tpr, thresholds = roc_curve(aucdfgbdt['yuqiday1'], aucdfgbdt['gbdt'], pos_label=1)
    # # print 'gbdt', auc(fpr, tpr)
    a = [2, 4, 6, 8]
    aucdfw = aucdf.sort_values(by='sfscore1', ascending=False)
    aucdfm = aucdf.sort_values(by='sfscore2',ascending=False)
    print aucdfw.head()
    # print aucdfm.head()
    for i in a:
        aucpassw1 = aucdfw.head(int((len(aucdfw) * i) / 10))
        aucpassw0 = aucdfw.drop(aucpassw1.index)
        print 'model1', i
        # print aucpassw1['yuqiday1'].value_counts()
        print len(aucpassw1)
        # wecashzhi = (len(aucpassw1[aucpassw1['yuqiday1'] != 0]) * 1.0) / len(aucpassw1)
        # print 'wecashyuqi', wecashzhi
        print '+++++++++++++++++'
        aucpassm1 = aucdfm.head(int((len(aucdfm) * i) / 10))
        aucpassm0 = aucdfm.drop(aucpassm1.index)
        print 'myselfpass', i
        # print aucpassm['yuqiday1'].value_counts()
        print len(aucpassm1)
        # myselfz = (len(aucpassm1[aucpassm1['yuqiday1'] != 0]) * 1.0) / len(aucpassm1)
        # print 'myselfyuqi', myselfz
        print '|||||||||||||||||||||'
        # print 'wecash-myself', wecashzhi - myselfz
        print '|||||||||||||||||||||'
        print 'model1 bad user'
        print aucpassw0['rulenum'].value_counts()
        print 'model1 '
        print aucpassw1['rulenum'].value_counts()
        print 'myself bad user'
        print  aucpassm0['rulenum'].value_counts()
        print 'myself '
        print  aucpassm1['rulenum'].value_counts()
        print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
    print '************************************************'
def xianguan():
    int_file = '/Users/ufenqi/Downloads/testuserallgbdt'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[3] == '' or linelist[4] == '' or linelist[-2] == '':
                continue
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['userid', 'time', 'passtf', 'wecash', 'myself', 'yuqiday', 'rulenum', 'gbdt'])
    # print aucdf.head()
    # aucdf = aucdf[aucdf['yuqiday1'] > -1]
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    # aucdf['myself'] = aucdf['myself'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    # aucdf['yuqiday1'] = aucdf['yuqiday']
    print aucdf['wecash'].corr(1-aucdf['gbdt'], method='pearson', min_periods=1)
def plthuitu():
    int_file = '/Users/ufenqi/Downloads/testuserallgbdt'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[3] == '' or linelist[4] == '' or linelist[-2] == '':
                continue
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['userid', 'time', 'passtf', 'wecash', 'myself', 'yuqiday', 'rulenum', 'gbdt'])
    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    # aucdf['myself'] = aucdf['myself'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']
    # aucdf=aucdf[aucdf['yuqiday1']>-1]
    print aucdf.__len__()
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    print 'wecash', auc(fpr, tpr)
    fpr_grd_lm, tpr_grd_lm, _ = roc_curve(aucdf['yuqiday1'], aucdf['wecash'])
    fpr_grd_lm1, tpr_grd_lm1, _ = roc_curve(aucdf['yuqiday1'], aucdf['myself'])
    plt.figure(1)
    plt.plot([0, 1], [0, 1], 'k--')

    # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
    plt.plot(fpr_grd_lm, tpr_grd_lm, label='wecash')
    plt.plot(fpr_grd_lm1, tpr_grd_lm1, label='myself')
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    plt.legend(loc='best')
def wecashauc():
    int_file = '/Users/ufenqi/Downloads/xuyonglong.txt'
    userlist = []
    dateyue={}
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            linelist.append(linelist[3][:6])
            dateyue[linelist[3][:6]]=0
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['dingdan', 'useridyuan', 'useridfeng', 'renzhengdate', 'wecash', 'yuqiday','dateyue'])
    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    # aucdf['myself'] = aucdf['myself'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 60)] = 1
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    print aucdf.__len__()
    for da in dateyue:
        print da
        aucdf1 = aucdf[aucdf['dateyue'] ==da]
        print aucdf1.__len__()
        fpr, tpr, thresholds = roc_curve(aucdf1['yuqiday1'], aucdf1['wecash'], pos_label=1)
        print 'wecash', auc(fpr, tpr)
        print "++++++++++++++++++++++++"


def testwec_11_15():
    int_file = '/Users/ufenqi/Downloads/feayuqiself501'
    userlist=[]
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            userlist.append(linelist)
            # print len(linelist)
    aucdf=pd.DataFrame(userlist,columns=['phone','wecash','g1','yuqiday','myself'])
    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('int')
    aucdf['myself'] = aucdf['myself'].astype('float')
    aucdf['wecash'] = aucdf['wecash'].astype('float')
    aucdf['g1'] = aucdf['g1'].astype('float')
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1']=aucdf['yuqiday']
    # aucdf=aucdf[aucdf['yuqiday1']>-1]
    print aucdf.__len__()
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    # aucdf['yuqiday1'][aucdf['g1'] >0] = 0
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    print aucdf['yuqiday1'].value_counts()
    # fpr, tpr, thresholds = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    # print 'wecash', auc(fpr, tpr)

    aucdfw = aucdf.sort_values(by='wecash', ascending=False)
    aucdfm = aucdf.sort_values(by='myself', ascending=False)
    yt = aucdfw['yuqiday1'].values.tolist()
    wel = aucdfw['wecash'].values.tolist()
    print 'wecash'
    for i in range(0, 10):
        num = 0
        index1 = int(0.1 * i * len(yt))
        # print wel[index1], index1
        for ui in range(0, len(yt)):

            if wel[ui] > wel[index1]:
                if yt[ui] == 1:
                    num += 1
            else:
                if yt[ui] == 0:
                    num += 1
        # print num
        print 'passrate:', i * 0.1, 'acc:', 1.0 * num / len(yt)
    print "**********************"
    print 'myself'
    yt = aucdfm['yuqiday1'].values.tolist()
    wel = aucdfm['myself'].values.tolist()
    for i in range(0, 10):
        num = 0
        index1 = int(0.1 * i * len(yt))
        # print wel[index1],index1
        for ui in range(0, len(yt)):
            if wel[ui] > wel[index1]:
                if yt[ui] == 1:
                    num += 1
                    # print '1111',wel[ui]
            else:
                if yt[ui] == 0:
                    num += 1
        # print num
        print 'passrate:', i * 0.1, 'acc:', 1.0 * num / len(yt)
    # wel.sort()
    index1 = int(0.4 * len(yt))
    print wel[index1]
    # plt.figure(2)
    # n, bins, patches = plt.hist(wel, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
def passingrate():
    int_file = '/Users/ufenqi/Downloads/wecashq711'
    userphn = []
    userscore=[]
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            if linelist[2] == '':
                linelist[2] = '0'
            userphn.append(linelist[0])
            userscore.append(float(linelist[-1])-0.49)
    for i in range(0, 10):
        num = 0
        for ui in range(0, len(userphn)):
            # index1 = int(0.1 * i * len(y_predprobl))
            if userscore[ui] > 0.1 * i:
                num += 1
        print 'scor:', i * 0.1, 'passrate:', 1.0 * num / len(userphn)
    for ui in range(0, len(userphn)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if userscore[ui] > 0.1 * 2.6:
            num += 1
    print 'scor:', 2.6 * 0.1, 'passrate:', 1.0 * num / len(userphn)
def describute():
    # sfile='/Users/ufenqi/Documents/dataming/base1/rule/scorecodenordelphnallpassall.csv'
    # tdf=pd.read_csv(sfile,header=None,names=['phn','score'])
    # y_predprobl=tdf['score'].values.tolist()
    # print tdf.__len__()
    # for i in range(0,10):
    #     num=0
    #     for ui in range(0,len(tdf)):
    #         # index1 = int(0.1 * i * len(y_predprobl))
    #         if y_predprobl[ui]>0.1 * i:
    #             num+=1
    #     print 'scor:',i*0.1,'pass:',1.0*num/len(tdf)
    #
    # for ui in range(0,len(tdf)):
    #     # index1 = int(0.1 * i * len(y_predprobl))
    #     if y_predprobl[ui]>0.1 * 2.6:
    #         num+=1
    # print 'scor:',2.6,'pass:',1.0*num/len(tdf)
    # y_predprobl=[]
    s_file='/Users/ufenqi/Documents/dataming/base2/overdue/score_td_815'
    userlist = []
    with open(s_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[1] == 'NULL' or linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            # linelist.remove(linelist[0])
            userlist.append(linelist)
            # print len(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['phn', 'daytime', 'sfscore', 'tdscore', 'yuqiday'])

    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['tdscore'] = aucdf['tdscore'].astype('float')
    aucdf['sfscore'] = aucdf['sfscore'].astype('float')
    # print aucdf.describe()
    # aucdf['gbdt'] = aucdf['gbdt'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']
    aucdf['yuqiday2'] = aucdf['yuqiday']
    # aucdf = aucdf[aucdf['yuqiday1'] > 29]
    print len(aucdf)
    # aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    # aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    # print "sfscore AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['sfscore'])
    # aucdf['yuqiday2'][(aucdf['yuqiday2'] > -1) & (aucdf['yuqiday2'] < 30)] = 0
    # aucdf['yuqiday2'][aucdf['yuqiday2'] != 0] = 1
    # print "sfscoretd AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday2'], aucdf['tdscore'])

    # print aucdf['tdscore'].value_counts()
    # for i in range(0,10):
    #     num=0
    #     for ui in range(0,len(y_predprobl)):
    #         # index1 = int(0.1 * i * len(y_predprobl))
    #         if y_predprobl[ui]>0.1 * i:
    #             num+=1
    #     print 'scor:',i*0.1,'acc:',1.0*num/len(y_predprobl)
    # num=0
    # for ui in range(0,len(y_predprobl)):
    #     # index1 = int(0.1 * i * len(y_predprobl))
    #     if y_predprobl[ui]>0.1 * 7.5:
    #         num+=1
    # print 'scor:',0.75,'acc:',1.0*num/len(y_predprobl)
    y_predprobl = aucdf['sfscore'].values.tolist()
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
def fea_plt():
    s_file = '/Users/ufenqi/Documents/dataming/model_yu/data/score_10252.csv'
    aucdf=pd.read_csv(s_file)
    # aucdf = aucdf[aucdf['yuqiday1'] > 29]
    col=['110','111','112','113','114','115','116']
    print len(aucdf)
    print aucdf[col].describe().T
    # y_predprobl = aucdf['112'].values.tolist()
    # plt.figure(1)
    # n, bins, patches = plt.hist(y_predprobl, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # plt.show()

if __name__ == '__main__':

    # aucmyself()
    #
    # quchongmax()
    # auctransition()
    # auctransition3()
    # maxuserid()
    aucscore()
    # split_date()
    # wecashfenbu()
    # guizenum()
    # xianguan()
    # plthuitu()
    # wecashauc()
    # testwec_11_15()
    # passingrate()
    # describute()
    # fea_plt()
    # auctaobao()