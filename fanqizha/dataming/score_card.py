# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
import math
import matplotlib.pylab as plt
sys.setdefaultencoding('utf-8')
#计算分数
def compute_score():
    alpha=360
    beta=40
    binning_file=dconf.config_path+'binning.json'
    woe_file=dconf.config_path+'woe_value.json'
    weight_fiel=dconf.config_path+'score_card_weight.json'
    feature_name_file=dconf.config_path+'featurename_online_use_score_card'
    score_card_file=dconf.config_path+'score_card.json'
    wfp=open(score_card_file,'w')
    fea_name_list=[]
    with open(feature_name_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            fea_name_list.append(linelist[0])
    with open(binning_file, 'r') as fp:
        cc = fp.readline()
        b_dics = json.loads(cc)
    print b_dics
    with open(woe_file, 'r') as fp:
        cc = fp.readline()
        woe_dics = json.loads(cc)
    print woe_dics
    with open(weight_fiel, 'r') as fp:
        cc = fp.readline()
        w_dics = json.loads(cc)
    print w_dics
    column_dic={}
    column_dic['base_score']=alpha
    for col in fea_name_list:
        print col
        col_dic={}
        col_list=[]
        bin_list=b_dics[col]
        woe_dic=woe_dics[col]
        weight=w_dics[col]
        for wi in range(10):
            w_col=str(wi)
            col_key=str(bin_list[wi])+'^'+str(bin_list[wi+1])
            col_value=round((woe_dic[w_col]*weight*beta)/math.log(2),0)
            print col_key,col_value
            # col_dic[col_key]=col_value
            col_list.append([round(bin_list[wi],10),round(bin_list[wi+1],10),col_value])
        column_dic[col]=col_list
    bias=w_dics['bias']
    bias_score=round((bias*beta)/math.log(2),0)
    column_dic['bisa_score']=bias_score
    print 'bisa_score',bias_score
    json.dump(column_dic,wfp)
#  计算参数
def calculate_parameter(basepoints,baseodds,pdo):
    beta=pdo/math.log(2)
    alpha=basepoints-beta*math.log(baseodds)
    print '参数1:',alpha
    print '参数2:',beta
# 评分卡预测
def score_pre(file_name):
    feature_name_file = dconf.config_path + 'featurename_online_use_score_card'
    score_card_file = dconf.config_path + 'score_card.json'
    init_file=dconf.data_path+file_name
    out_file = dconf.data_path + 'score' + file_name
    wfp=open(out_file,'w')
    fea_name_list = []
    testdfall = pd.read_csv(init_file)
    with open(feature_name_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            fea_name_list.append(linelist[0])
    with open(score_card_file, 'r') as fp:
        cc = fp.readline()
        score_card_dic = json.loads(cc)
    score_dic={}
    for uid in testdfall['0'].values.tolist():
        uiddf=testdfall[testdfall['0'] == uid]
        # print uiddf
        for col in fea_name_list:
            score_dic.setdefault(uid,0)
            col_value=uiddf[col].values.tolist()[0]
            # print col_value
            col_data = score_card_dic[col][0]
            # 评分卡开头
            if col_value < col_data[1]:
                score_dic[uid] += col_data[2]
            else:
                # 评分卡结尾
                col_data = score_card_dic[col][-1]
                if col_value >= col_data[0]:
                    score_dic[uid] += col_data[2]
                else:
                    for col_data in score_card_dic[col][1:-1]:
                        if col_value>=col_data[0] and col_value<col_data[1]:
                            score_dic[uid]+=col_data[2]
                            break
        # bisa分
        score_dic[uid]+=score_card_dic['bisa_score']
        # 基础分
        score_dic[uid] += score_card_dic['base_score']
    for ui in score_dic:
        wfp.write(str(ui)+','+str(score_dic[ui])+'\n')
    y_predprobl=score_dic.values()
    # 预测值分布图
    plt.figure(1)
    n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    plt.grid(True)
    plt.show()
def score_distribution():
    init_file=dconf.data_path+'tx_all'
    init_list=[]
    with open(init_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            init_list.append(linelist)
    scoredf=pd.DataFrame(init_list,columns=['uid','score','time','overday'])
    scoredf['overday'] = scoredf['overday'].astype('float')
    scoredf['score'] = scoredf['score'].astype('float')
    scoredf['overday1'] = scoredf['overday']
    scoredf = scoredf[scoredf['overday1'] > -1]
    scoredf['score_cut'], arrylist = pd.qcut(scoredf['score'], 10, labels=False, retbins=True, duplicates='drop')
    # scoredf.to_csv(dconf.data_path+'score_card_test.csv')
    # print scoredf['score_cut'].value_counts()
    # print scoredf.head()
    print arrylist
    for li in arrylist:
        print li
    print '***************'
    for li in arrylist:
        print li+1
    all_num=len(scoredf)
    sum_1=0
    sum_3=0
    sum_30=0
    for i in range(10):
        day_list=scoredf['overday1'][scoredf['score_cut']==i].values.tolist()
        # print scoredf[scoredf['score_cut']==i].head()
        len_num=len(day_list)
        len_1=0
        len_3=0
        len_30=0
        for dl in day_list:
            if dl>=30:
                len_1+=1
                len_3+=1
                len_30+=1
            elif dl>=3:
                len_1 += 1
                len_3 += 1
            elif dl>=1:
                len_1 += 1
        sum_1+=len_1
        sum_3 += len_3
        sum_30 += len_30
        print len_num,len_1,round(len_1*1.0/len_num,4),len_3,round(len_3*1.0/len_num,4),len_30,round(len_30*1.0/len_num,4),i
    print all_num,sum_1,round(sum_1*1.0/all_num,4),sum_3,round(sum_3*1.0/all_num,4),sum_30,round(sum_30*1.0/all_num,4)


if __name__ == '__main__':
    file_name = 'test_score_test_9021.csv'
    # compute_score()
    # score_pre(file_name)
    # calculate_parameter(600,0.95,20)
    score_distribution()
