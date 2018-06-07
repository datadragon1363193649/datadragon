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
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
def funnel(file_name):
    def r_num(id_list,rule_dic):
        rule1_dic = {k: v for k, v in rule_dic.items() if v > 1}
        id_list=[x for x in id_list if x not in rule1_dic.keys()]
        return id_list
    pass_id_list=[]
    td_id_list=[]
    self_id_list=[]
    # m0_other_id_list=[]
    admittance_id_list=[]
    anti_fraud_id_list=[]
    black_id_list=[]
    m1_id_list=[]
    m1_other_id_list=[]
    card_id_list=[]
    admittance_rule_list=['face无源人脸识别','年龄限制','身份证所属拒绝省',
                          '手机归属拒绝省','身份证所属拒绝市',
                          '手机归属拒绝市','身份证有效期']
    anti_fraud_rule_list=['手机在网时长','客户与紧急联系人联系次数',
                          '近6个月通话总次数','近6个月通话总时长',
                          '客户通讯录通话详单D逾期客户个数异常',
                          '客户通讯录通话详单M逾期客户个数异常',
                          '百度网查结果','运营商实名情况']
    black_rule_list=['内部黑名单-身份证命中内部黑名单',
                     '内部黑名单-手机号命中内部黑名单','中智诚黑名单',
                     '百融黑名单','民族黑名单','闪银黑名单','IP黑名单']
    use_rule_list=['同盾分过高直拒','内部欺诈分','国政通人脸识别',
                   '闪银-客户累计逾期次数简单借款','腾讯分','氪信分'
                   ,'内部反欺诈M1','风控信用评分']
    rule_dic={}
    in_file=dconf.data_path+file_name
    id_dic={}
    with open(in_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            id_dic[linelist[0]]=0
            if linelist[3]=='':
                pass_id_list.append(linelist[0])
            elif linelist[3] in use_rule_list:
                rule_dic.setdefault(linelist[0], 0)
                rule_dic[linelist[0]]+=1
                if linelist[3]=='同盾分过高直拒':
                    td_id_list.append(linelist[0])
                if linelist[3]=='内部欺诈分':
                    self_id_list.append(linelist[0])
                if linelist[3]in ['国政通人脸识别',
                                  '闪银-客户累计逾期次数简单借款',
                                  '腾讯分','氪信分']:
                    m1_other_id_list.append(linelist[0])
                if linelist[3]=='内部反欺诈M1':
                    m1_id_list.append(linelist[0])
                if linelist[3]=='风控信用评分':
                    card_id_list.append(linelist[0])
            elif linelist[3] in admittance_rule_list:
                rule_dic.setdefault(linelist[0], 0)
                rule_dic[linelist[0]] += 1
                admittance_id_list.append(linelist[0])
            elif linelist[3] in anti_fraud_rule_list:
                rule_dic.setdefault(linelist[0], 0)
                rule_dic[linelist[0]] += 1
                anti_fraud_id_list.append(linelist[0])
            elif linelist[3] in black_rule_list:
                rule_dic.setdefault(linelist[0], 0)
                rule_dic[linelist[0]] += 1
                black_id_list.append(linelist[0])
    fp.close()
    id_len=len(id_dic)
    id_len_update = len(id_dic)
    fun_list=[]
    print '总数',id_len

    admittance_id_list=list(set(admittance_id_list))
    admittance_only_list = [x for x in admittance_id_list if x
                    not in anti_fraud_id_list+black_id_list+
                            td_id_list+self_id_list]
    fun_list = list(set(fun_list + admittance_id_list))
    # m0_other_id_list=[x for x in m0_other_id_list if x not in td_id_list ]
    # m0_other_id_list = [x for x in m0_other_id_list if x not in self_id_list]
    print 'admittance_only_list',len(admittance_id_list),\
        len(admittance_id_list)*1.0/id_len_update,len(admittance_only_list),\
        len(admittance_only_list)*1.0/id_len,len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update

    anti_fraud_id_list = list(set(anti_fraud_id_list))
    anti_fraud_only_list = [x for x in anti_fraud_id_list if x
                            not in admittance_id_list + black_id_list +
                            td_id_list + self_id_list]
    fun_list = list(set(fun_list + anti_fraud_id_list))
    print 'anti_fraud_only_list', len(anti_fraud_id_list),\
        len(anti_fraud_id_list)*1.0/id_len_update,len(anti_fraud_only_list), \
        len(anti_fraud_only_list) * 1.0 / id_len, len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update

    black_id_list = list(set(black_id_list))
    black_only_list = [x for x in black_id_list if x
                            not in admittance_id_list + anti_fraud_id_list
                       + td_id_list + self_id_list]
    fun_list = list(set(fun_list + black_id_list))
    print 'black_only_list', len(black_id_list),\
        len(black_id_list)*1.0/id_len_update,len(black_only_list), \
        len(black_only_list) * 1.0 / id_len, len(fun_list), \
        1-(len(fun_list)*1.0/id_len),id_len_update

    fun_list=list(set(fun_list+td_id_list))
    td_only_list = r_num(td_id_list, rule_dic)
    print 'td_id_list', len(td_id_list),\
        len(td_id_list)*1.0/id_len_update,len(td_only_list), \
        len(td_only_list) * 1.0 / id_len,len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update

    fun_list = list(set(fun_list + self_id_list))
    self_only_list = r_num(self_id_list, rule_dic)
    print 'self_id_list', len(self_id_list),\
        len(self_id_list)*1.0/id_len_update,len(self_only_list), \
        len(self_only_list) * 1.0 / id_len,len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update
    wfp = open(dconf.data_path+'m0','w')
    for so in self_only_list:
        wfp.write(so+"\n")
    wfp.close()
    id_dic=[x for x in id_dic if x not in fun_list]
    id_len_update = len(id_dic)
    fun_list = list(set(fun_list + m1_other_id_list))
    m1_other_id_list = list(set(m1_other_id_list))
    print 'm1_other_id_list',len(m1_other_id_list),\
        len(m1_other_id_list)*1.0/id_len_update, len(m1_other_id_list), \
        len(m1_other_id_list) * 1.0 / id_len,len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update

    id_dic = [x for x in id_dic if x not in fun_list]
    id_len_update = len(id_dic)
    fun_list = list(set(fun_list + m1_id_list))
    m1_id_list = r_num(m1_id_list, rule_dic)
    print 'm1_id_list', len(m1_id_list),\
        len(m1_id_list)*1.0/id_len_update,len(m1_id_list), \
        len(m1_id_list) * 1.0 / id_len,len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update

    id_dic = [x for x in id_dic if x not in fun_list]
    id_len_update = len(id_dic)
    fun_list = list(set(fun_list + card_id_list))
    card_id_list = r_num(card_id_list, rule_dic)
    print 'card_id_list', len(card_id_list),\
        len(card_id_list)*1.0/id_len_update,len(card_id_list), \
        len(card_id_list) * 1.0 / id_len,len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update

    id_dic = [x for x in id_dic if x not in fun_list]
    id_len_update = len(id_dic)
    fun_list = list(set(fun_list + pass_id_list))
    print 'pass_id_list',len(pass_id_list),\
        len(pass_id_list)*1.0/id_len_update, len(pass_id_list),\
        len(pass_id_list) * 1.0 / id_len,len(fun_list),\
        1-(len(fun_list)*1.0/id_len),id_len_update
if __name__ == '__main__':
    file_name='rule_5_24'
    funnel(file_name)