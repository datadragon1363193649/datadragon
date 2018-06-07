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
from sklearn.metrics import r2_score
import offline_db_conf as dconf
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

# 通过卡不同分数的到相应的混淆矩阵
def confusion_matrix():
    int_file = '/Users/ufenqi/Documents/dataming/model_yu/data/score_m1_825'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            userlist.append(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['phn', 'time', 'sfscore1','yuqiday','sfscore2'])
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['sfscore2'] = aucdf['sfscore2'].astype('float')
    aucdf['sfscore1'] = aucdf['sfscore1'].astype('float')
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
# 计算auc、ks、正负样本分布图等
def auc_score():
    # int_file=dconf.data_path+'m1_score'
    int_file = dconf.data_path + 'sc'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[1] == 'NULL' or linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            # linelist.remove(linelist[0])
            userlist.append(linelist)
    aucdf = pd.DataFrame(userlist,columns=['phn', 'time','yuqiday', 'sfscore1'])
    # aucdf = pd.DataFrame(userlist,columns=['uid','sfscore1','time','yuqiday'])

    # print aucdf.head()
    aucdf['yuqiday'] = aucdf['yuqiday'].astype('float')
    aucdf['sfscore1'] = aucdf['sfscore1'].astype('float')
    aucdf['yuqiday1'] = aucdf['yuqiday']
    aucdf = aucdf[aucdf['yuqiday1'] > -1]
    print len(aucdf)
    # aucdf=aucdf.drop(aucdf[(aucdf['yuqiday1']>=20) & (aucdf['yuqiday1']<=29)].index)
    print len(aucdf)
    aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] <30)] = 1
    aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    # aucdf=aucdf[aucdf['time']=='2018-04-25']
    print aucdf['time'].value_counts()
    print aucdf['sfscore1'].value_counts()
    print '************************************************'
    # 计算auc值
    # print "wecash AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['wecash'])
    # print "sfscore1 AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['sfscore1'])
    # print "sfscore2 AUC Score (Train): %f" % metrics.roc_auc_score(aucdf['yuqiday1'], aucdf['sfscore2'])

    # 绘制auc图使用
    # fpr_grd_lm, tpr_grd_lm, _ = roc_curve(aucdf['yuqiday1'], aucdf['wecash'], pos_label=1)
    fpr_grd_lm1, tpr_grd_lm1, _ = roc_curve(aucdf['yuqiday1'], aucdf['sfscore1'], pos_label=1)
    fpr_grd_lm2, tpr_grd_lm2, _ = roc_curve(aucdf['yuqiday1'], aucdf['sfscore1'], pos_label=1)
    aucdfw = aucdf.sort_values(by='sfscore1', ascending=False)
    yudaylist = aucdfw['yuqiday1'].values.tolist()
    # scorelist=aucdfw['wecash'].values.tolist()
    y_predprobl=aucdfw['sfscore1'].values.tolist()

    # 计算ks值
    get_ks = lambda y_pred, y_true: ks_2samp(y_pred[y_true == 1], y_pred[y_true != 1]).statistic
    print 'ks值', get_ks(aucdfw['sfscore1'], aucdfw['yuqiday1'])
    zhenglist=[]
    fulist=[]
    calist=[]
    xlist=[]
    zhengz=len(aucdfw[aucdfw['yuqiday1']==1])
    fuz = len(aucdfw[aucdfw['yuqiday1'] == 0])

    # 绘制ks值图使用
    # for i in range(0,100):
    #     lnum=int(len(aucdfw)*i/100)
    #     # print lnum
    #     adf=aucdfw.head(lnum)
    #     z=len(adf[adf['yuqiday1']==1])*1.0/zhengz
    #     f=len(adf[adf['yuqiday1'] == 0])*1.0/fuz
    #     zhenglist.append(z)
    #     fulist.append(f)
    #     calist.append(abs(z-f))
    #     # print i,abs(z-f)
    #     xlist.append(i)

    # 卡不同预测分数，计算通过率
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
        if y_predprobl[ui]>413:
            num+=1
    print 'scor:',413,'pass:',1.0*num/len(aucdfw)
    print 'scor:',413,'pass:',num
    num = 0
    for ui in range(0, len(aucdfw)):
        # index1 = int(0.1 * i * len(y_predprobl))
        if y_predprobl[ui] > 418:
            num += 1
    print 'scor:', 420, 'pass:', 1.0 * num / len(aucdfw)
    print 'scor:', 418, 'pass:', num
    # num = 0
    # for ui in range(0, len(aucdfw)):
    #     # index1 = int(0.1 * i * len(y_predprobl))
    #     if y_predprobl[ui] > 0.83:
    #         num += 1
    # print 'scor:', 0.83, 'pass:', 1.0 * num / len(aucdfw)
    # num = 0
    # for ui in range(0, len(aucdfw)):
    #     # index1 = int(0.1 * i * len(y_predprobl))
    #     if y_predprobl[ui] > 0.84:
    #         num += 1
    # print 'scor:', 0.84, 'pass:', 1.0 * num / len(aucdfw)

    # 卡不同预测分数，计算逾期率
    # yudf=aucdfw[aucdfw['sfscore1']>0.82]
    # yunum=len(yudf[yudf['yuqiday1']==0])
    # print '借款人数:',len(yudf)
    # print '逾期:',yunum*1.0/len(yudf)
    # yudf = aucdfw[aucdfw['sfscore1'] > 0.83]
    # yunum = len(yudf[yudf['yuqiday1'] == 0])
    # print '借款人数:', len(yudf)
    # print '逾期:', yunum * 1.0 / len(yudf)
    # yudf = aucdfw[aucdfw['sfscore1'] > 0.84]
    # yunum = len(yudf[yudf['yuqiday1'] == 0])
    # print '借款人数:', len(yudf)
    # print '逾期:', yunum * 1.0 / len(yudf)

    # 好坏展示图
    # red, blue = sns.color_palette("Set1", 2)
    # print aucdf.head()
    # sns.kdeplot(aucdf['sfscore1'][aucdf['yuqiday1'] == 1], shade=True, color=red)
    # sns.kdeplot(aucdf['sfscore1'][aucdf['yuqiday1'] == 0], shade=True, color=blue)
    # plt.show()

    # ks图
    # fig, ax = plt.subplots()
    # plt.plot(xlist, zhenglist, 'r', linewidth=2)
    # plt.plot(xlist, fulist, 'g', linewidth=2)
    # plt.plot(xlist, calist, 'b', linewidth=2)
    # plt.ylim(ymin=0)
    # plt.show()


    # auc图
    # plt.figure(1)
    # plt.plot([0, 1], [0, 1], 'k--')
    # # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
    # # plt.plot(fpr_grd_lm, tpr_grd_lm, label='wecash')
    # # plt.plot(fpr_grd_lm1, tpr_grd_lm1, label='myself1')
    # plt.plot(fpr_grd_lm2, tpr_grd_lm2,label='myself2')
    # plt.xlabel('False positive rate')
    # plt.ylabel('True positive rate')
    # plt.legend(loc='best')
    # plt.show()
    # print aucdfw['sfscore2']
    # aucdfw['sfscore2']=np.log(aucdfw['sfscore2'])
    # print aucdfw['sfscore2']
    # y_predprobl = aucdfw['sfscore1'].values.tolist()


    # 预测值分布图
    # plt.figure(1)
    # n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
# 用户命中规则数分析
def rule_num():
    int_file = dconf.data_path+'xxz_tast_score_m0_num'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            userlist.append(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['id', 'time', 'm0','rulenum'])
    # print aucdf.head()
    # print aucdf['m0'].value_counts()
    aucdf['m0'] = aucdf['m0'].astype('float')
    # aucdf['m1'] = aucdf['m1'].astype('float')
    aucdf['rulenum'] = aucdf['rulenum'].astype('float')

    # aucdf=aucdf[aucdf['yuqiday1']>-1]
    # aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    # aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    a = [8.6, 9.7, 9.9]
    # aucdfw = aucdf.sort_values(by='m0', ascending=False)
    # aucdfm = aucdf.sort_values(by='m0',ascending=False)
    aucdfm = aucdf.sample(frac=1)
    print aucdfm.head()
    # aucdfm = aucdf.sort_values(by='m0', ascending=True)
    # print aucdfw.head()
    print aucdfm.head()
    for i in a:
        # aucpassw1 = aucdfw.head(int((len(aucdfw) * i) / 10))
        # aucpassw0 = aucdfw.drop(aucpassw1.index)
        # print 'model1', i
        # print aucpassw1['yuqiday1'].value_counts()
        # print len(aucpassw1)
        # wecashzhi = (len(aucpassw1[aucpassw1['yuqiday1'] != 0]) * 1.0) / len(aucpassw1)
        # print 'wecashyuqi', wecashzhi
        # print '+++++++++++++++++'
        aucpassm1 = aucdfm.head(int((len(aucdfm) * i) / 10))
        aucpassm0 = aucdfm.drop(aucpassm1.index)
        print 'd_pass', str(i*10)+"%"
        # print aucpassm['yuqiday1'].value_counts()
        print len(aucpassm1)
        # myselfz = (len(aucpassm1[aucpassm1['yuqiday1'] != 0]) * 1.0) / len(aucpassm1)
        # print 'myselfyuqi', myselfz
        print '|||||||||||||||||||||'
        # print 'wecash-myself', wecashzhi - myselfz
        print '|||||||||||||||||||||'
        # print 'model1 bad user'
        # print aucpassw0['rulenum'].value_counts()
        # print 'model1 '
        # print aucpassw1['rulenum'].value_counts()
        print 'd bad user'
        print  aucpassm0['rulenum'].value_counts()*1.0/len(aucdfm)
        print 'd '
        print  aucpassm1['rulenum'].value_counts()*1.0/len(aucdfm)
        print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
    print '************************************************'
# 根据分数统计规则
def rule_num_score():
    int_file = dconf.data_path+'xxz_tast_score_m_num'
    userlist = []
    with open(int_file, 'r') as fp2:
        for line in fp2:
            linelist = line.strip().split(',')
            # if linelist[4] == 'NULL' or linelist[5] == 'NULL' or linelist[-2] == 'NULL':
            #     continue
            userlist.append(linelist)
    aucdf = pd.DataFrame(userlist,
                         columns=['id', 'time', 'm0','rulenum'])
    # print aucdf.head()
    # print aucdf['m0'].value_counts()
    aucdf['m0'] = aucdf['m0'].astype('float')
    # aucdf['m1'] = aucdf['m1'].astype('float')
    aucdf['rulenum'] = aucdf['rulenum'].astype('float')

    # aucdf=aucdf[aucdf['yuqiday1']>-1]
    # aucdf['yuqiday1'][(aucdf['yuqiday1'] > -1) & (aucdf['yuqiday1'] < 30)] = 1
    # aucdf['yuqiday1'][aucdf['yuqiday1'] != 1] = 0
    a = [1, 2, 3]
    aucdfm = aucdf.sort_values(by='m0', ascending=False)
    # aucdfm = aucdf.sort_values(by='m0',ascending=True)
    # aucdfm = aucdf.sample(frac=1)
    print aucdfm.head()
    # aucdfm = aucdf.sort_values(by='m0', ascending=True)
    # print aucdfw.head()
    print aucdfm.head()
    for i in a:
        # aucpassw1 = aucdfw.head(int((len(aucdfw) * i) / 10))
        # aucpassw0 = aucdfw.drop(aucpassw1.index)
        # print 'model1', i
        # print aucpassw1['yuqiday1'].value_counts()
        # print len(aucpassw1)
        # wecashzhi = (len(aucpassw1[aucpassw1['yuqiday1'] != 0]) * 1.0) / len(aucpassw1)
        # print 'wecashyuqi', wecashzhi
        # print '+++++++++++++++++'
        # aucpassm1 = aucdfm.head(int((len(aucdfm) * i) / 10))
        # aucpassm0 = aucdfm.drop(aucpassm1.index)
        aucpassm1 = aucdfm[aucdfm['m0']<i]
        aucpassm0 = aucdfm.drop(aucpassm1.index)
        print 'd_pass', str((len(aucpassm1)*1.0/len(aucdfm)*100))+"%"
        # print aucpassm['yuqiday1'].value_counts()
        print len(aucpassm1)
        # myselfz = (len(aucpassm1[aucpassm1['yuqiday1'] != 0]) * 1.0) / len(aucpassm1)
        # print 'myselfyuqi', myselfz
        print '|||||||||||||||||||||'
        # print 'wecash-myself', wecashzhi - myselfz
        print '|||||||||||||||||||||'
        # print 'model1 bad user'
        # print aucpassw0['rulenum'].value_counts()
        # print 'model1 '
        # print aucpassw1['rulenum'].value_counts()
        print 'd bad user'
        print  aucpassm0['rulenum'].value_counts()*1.0/len(aucdfm)
        print 'd '
        print  aucpassm1['rulenum'].value_counts()*1.0/len(aucdfm)
        print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
    print '************************************************'

#数字预测模型分析评估方法
def num_assess():
    s_file = '/Users/ufenqi/Documents/dataming/earnings/data/score511_p_call_class'
    true = []
    pred=[]
    ulist=[]
    with open(s_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            true.append(float(linelist[1]))
            pred.append(float(linelist[-1]))
            ulist.append([float(linelist[1]),float(linelist[-1])])
    udf=pd.DataFrame(ulist,columns=['true','pred'])
    udfw = udf.sort_values(by='pred', ascending=False)
    ppred=udfw['pred'].values.tolist()
    ptrue = udfw['true'].values.tolist()
    xlist=range(len(ptrue))
    print "MAE:", metrics.mean_absolute_error(true, pred)

    # calculate MSE using scikit-learn
    print "MSE:", metrics.mean_squared_error(true, pred)

    # calculate RMSE using scikit-learn
    print "RMSE:", np.sqrt(metrics.mean_squared_error(true, pred))
    print 'R2',r2_score(true,pred)
    # plt.figure(1)
    # n, bins, patches = plt.hist(true, 100, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # plt.show()
    fig, ax = plt.subplots()
    plt.scatter(xlist, ptrue, color ='r')
    plt.scatter(xlist, ppred, color ='g')
    plt.ylim(ymin=0)
    plt.show()
def get_rule_num():
    in_file=dconf.data_path+'xxz_tast_score_m0'
    out_file=dconf.data_path+'xxz_tast_score_m0_num'
    wfp=open(out_file,'w')
    rule_num_dic={}
    with open(in_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            rname=linelist[1]+','+linelist[2]+','+linelist[4]
            rule_num_dic.setdefault(rname,0)
            rule_num_dic[rname]+=1
    for rule in rule_num_dic:
        wfp.write(rule+','+str(rule_num_dic[rule])+'\n')
    wfp.close()
def rule_black():
    in_file = dconf.data_path + 'jdjk_rule'
    rule_num_dic = {}
    self_num=0
    td_num=0
    black_dic={}
    call_dic={}
    import_num=0
    n_black_dic={}
    with open(in_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            if linelist[2] in ['内部欺诈分']:
                self_num+=1
            if linelist[2] in ['同盾分过高直拒']:
                td_num+=1
            if linelist[2] in ['百融黑名单','命中中智诚黑名单','中智诚黑名单',
                               '命中百融黑名单','闪银黑名单','闪银黑名单直拒']:
                black_dic.setdefault(linelist[0],0)
                black_dic[linelist[0]]+=1
            if linelist[2] in ['近6个月通话总次数','近6个月通话总时长']:
                call_dic.setdefault(linelist[0], 0)
                call_dic[linelist[0]] += 1
            if linelist[2] in ['客户与紧急联系人联系次数']:
                import_num += 1
            if linelist[2] in ['内部黑名单-手机号命中内部黑名单','内部黑名单-身份证命中内部黑名单']:
                n_black_dic.setdefault(linelist[0], 0)
                n_black_dic[linelist[0]] += 1
            rname = linelist[0] + ',' + linelist[1] + ',' + linelist[3]
            rule_num_dic.setdefault(rname, 0)
    print 'self_num', self_num, len(rule_num_dic), self_num * 1.0 / len(rule_num_dic)
    print 'td_num', td_num, len(rule_num_dic), td_num * 1.0 / len(rule_num_dic)
    print 'black_num',len(black_dic),len(rule_num_dic),len(black_dic)*1.0/len(rule_num_dic)
    print 'call_num',len(call_dic), len(rule_num_dic), len(call_dic) * 1.0 / len(rule_num_dic)
    print 'import_num', import_num, len(rule_num_dic), import_num * 1.0 / len(rule_num_dic)
    print 'n_black_num', len(n_black_dic), len(rule_num_dic), len(n_black_dic) * 1.0 / len(rule_num_dic)
if __name__ == '__main__':
    # confusion_matrix()
    auc_score()
    # get_rule_num()
    # rule_num()
    # num_assess()
    # rule_black()
    # rule_num_score()
