# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
from sklearn import cross_validation, metrics
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
def num():
    in_file='/Users/ufenqi/Documents/dataming/phn_risk/dataset/rule_con'
    uid_rule_dic={}
    rule_name={}
    user_num={}
    with open(in_file) as fp:
        for line in fp:
            linelist=line.strip().split(',')
            uid_rule_dic.setdefault(linelist[1],[])
            uid_rule_dic[linelist[1]].append(linelist[2])
            rule_name.setdefault(linelist[2],0)
            user_num.setdefault(linelist[1],0)
            user_num[linelist[1]]+=1
    for user in user_num:
        if user_num[user]<2:
            print user,uid_rule_dic[user][0],len(uid_rule_dic[user])
    name_list=[]
    for name in rule_name:
        name_list.append(name)
    user_list=[]
    for uid in uid_rule_dic:
        rule_list= uid_rule_dic[uid]
        u_list=[uid]
        for name in name_list:
            if name in rule_list:
                u_list.append('yes')
            else:
                u_list.append('no')
        user_list.append(u_list)
    user_df=pd.DataFrame(user_list,columns=['id']+name_list)
    # print user_df.head()
    user_df.to_csv('/Users/ufenqi/Documents/dataming/phn_risk/dataset/rule_df.csv',index=False)
def tongdun():
    in_file='/Users/ufenqi/Documents/dataming/phn_risk/dataset/score.txt'
    tongdunlist=[]
    with open(in_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split('^')
            if linelist[-1]=='':
                linelist[-1]='-1'
            tongdunlist.append([linelist[0],linelist[1],linelist[3],linelist[-1]])
    tdf=pd.DataFrame(tongdunlist,columns=['id','time','is_pass','score'])
    tdf['score'] = tdf['score'].astype('float')
    print tdf.head()
    tdf.to_csv('/Users/ufenqi/Documents/dataming/phn_risk/dataset/tongdun_score.csv',index=False)
def monitor():
    in_file = '/Users/ufenqi/Documents/dataming/phn_risk/dataset/rule_con'
    uid_rule_dic = {}
    user_name = {}
    user_list_real={}
    user_list_td={}
    user_list_nb={}
    with open(in_file) as fp:
        for line in fp:
            linelist = line.strip().split(',')
            if linelist[2] in ['客户通讯录通话详单D逾期客户个数异常','内部黑名单-手机号命中内部黑名单',
                               '内部黑名单-身份证命中内部黑名单','中智诚黑名单','百融黑名单',
                               '客户通讯录通话详单M逾期客户个数异常','闪银-客户累计逾期次数简单借款']:
                user_list_real.setdefault(linelist[1],0)
            if linelist[2] == '同盾分过高直拒':
                user_list_td.setdefault(linelist[1],0)
            if linelist[2] == '内部欺诈分':
                user_list_nb.setdefault(linelist[1],0)
            user_name.setdefault(linelist[1],0)
    td_real=[]
    td_pre = []
    nb_real = []
    nb_pre = []
    for name in user_name:
        if name in user_list_td:
            if name in user_list_real:
                td_real.append(0)
                td_pre.append(0)
        if name in user_list_td:
            if name not in user_list_real:
                # td_score.append(1,0)
                td_real.append(1)
                td_pre.append(0)
        if name in user_list_real:
            if name not in user_list_td:
                # td_score.append(0,1)
                td_real.append(0)
                td_pre.append(1)

        if name in user_list_nb:
            if name in user_list_real:
                # td_score.append(0,0)
                nb_real.append(0)
                nb_pre.append(0)
        if name in user_list_nb:
            if name not in user_list_real:
                # td_score.append(1,0)
                nb_real.append(1)
                nb_pre.append(0)
        if name in user_list_real:
            if name not in user_list_nb:
                # td_score.append(0,1)
                nb_real.append(0)
                nb_pre.append(1)
    print "tongdun AUC Score (Train): %f" % metrics.roc_auc_score(td_real, td_pre)
    print "self AUC Score (Train): %f" % metrics.roc_auc_score(nb_real, nb_pre)
    print len(user_list_real)
    print len(user_name)
if __name__ == '__main__':
    # tongdun()
    # num()
    monitor()