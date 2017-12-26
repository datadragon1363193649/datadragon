#! /usr/bin/env python
# -*- cocoding: utf-8 -*-

import sys
import os
import time
import datetime
import numpy as np
import math
import json
from scipy import stats
from sklearn.utils.multiclass import type_of_target
from sklearn.decomposition import PCA

class FeaEtl(object):
    "process feature"
    def __init__(self):
        pass
        
    def pro_lable(self, basic_days):
        """处理lable数据
        
        Input: 
        lable data: userid  overduedays
        basic_days: 按照几天作为正负划分标准
        
        Output: 
        userid lable # 0:好 1:坏
        """
        input_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test1/sample/s_raw'
        output_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/sample/s_' + str(basic_days)
        stat_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/sample/pn_stat'
        uinfo = {}
        with open(input_file, 'r') as rfp:
            for line in rfp:
                linelist = line.strip().split('\t')
                userid, overduedays = linelist
                overduedays = int(overduedays)
                uinfo.setdefault(userid, []).append(overduedays)
        p = 0
        n = 0
        with open(output_file, 'w') as wfp:
            for k, v in uinfo.items():
                sv = len(v)
                sd = reduce(lambda x,y:x+y, v)
                md = int(1.0*sd/sv)
                lable = 0
                if md > basic_days:
                    lable = 1
                if lable == 0:
                    p += 1
                else:
                    n += 1
                wfp.write(k + '\t' + str(lable) + '\n')
        pro = round(1.0*p/n, 2)
        if not os.path.exists(stat_file):
            fp = open(stat_file, 'w')
            fp.close()
        with open(stat_file, 'a') as wfp:
            content = "s_" + str(basic_days) + ': ' + \
                      'positive: ' + str(p) + '  ' + \
                      'negative: ' + str(n) + '  ' + \
                      'proportion: ' + str(pro) + '\n'
            wfp.write(content)
    
    def pro_fea_1(self):
        input_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/lhp'
        output_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_1'
        s_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/sample/s'
        uid_dic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                uid, lbl = line.strip().split('\t')
                uid_dic[uid] = 0
        
        with open(input_file, 'r') as rfp, open(output_file, 'w') as wfp:
            for line in rfp:
                linelist = line.strip().split(',')
                uid = linelist[0]
                if uid_dic[uid] > 0:
                    continue
                uid_dic[uid] += 1
                gender = linelist[5]
                age = linelist[4]
                if gender == '男':
                    gender = '0'
                elif gender == '女':
                    gender = '1'
                else:
                    gender = '2'
                wfp.write(uid + '\t' + gender + '\t' + age + '\n')
    
    def pro_fea_2(self):
        pass
    def pro_lable1(self):
        output_file = self.output_dir + '/lable1_' + time.strftime("%Y%m%d%H%M%S", time.localtime())
        
    def pro_train(self):
        train_file = '/home/liuxiaoliang/workspace/feature/working/b_test/test_20161115_20161216'
        lable_file = '/home/liuxiaoliang/workspace/feature/working/b_test/uid_odd'
        output_file = '/home/liuxiaoliang/workspace/feature/working/b_test/test_' + time.strftime("%Y%m%d%H%M%S", time.localtime())
        lable = {}
        with open(lable_file, 'r') as lfp:
            for line in lfp:
                userid, odd = line.strip().split('\t')
                lable[userid] = int(odd)
        wfp = open(output_file, 'w')
        with open(train_file, 'r') as tfp:
            for line in tfp:
                try:
                    linelist = line.replace('"', '').strip().split(',')
                    userid = linelist[0]
                    re_list = []
                    if userid not in lable:
                        continue
                    odd = lable[userid]
                    status = linelist[2]
                    if status == 'basic_auth_app':
                        status = 1
                    else:
                        status = 2
                    age = linelist[3].strip()
                    if age == '':
                        age = 0
                    else:
                        age = int(age)
                    gender = linelist[4]
                    if gender == '男':
                        gender = 0
                    elif gender == '女':
                        gender = 1
                    else:
                        gender = 2
                    degree = linelist[5]
                    if (degree.find('专升本') >= 0 or
                        degree.find('夜大') >= 0 or
                        degree.find('电大') >= 0 or
                        degree.find('涵大') >= 0):
                        degree = 0
                    elif degree.find('专科') >= 0:
                        degree = 1
                    elif (degree.find('本科') >= 0 or
                          degree.find('第二学士学位') >= 0):
                        degree = 2
                    elif degree.find('硕士') >= 0:
                        degree = 3
                    elif degree.find('博士') >= 0:
                        degree = 4
                    else:
                        degree = 5
                    face = linelist[6]
                    contacts_num = linelist[7] # 通讯录个数
                    sensword_num = linelist[8] # 敏感词个数
                    name_matched = linelist[9] # 姓名是否与运营商数据匹配
                    ic_matched = linelist[10] # 身份证号码是否与运营商数据匹配
                    phone_call_count_sum = linelist[11] # 近6个月通话总次数
                    phone_call_time_sum = linelist[12] # 近6个月通话总时长
                    phone_online_time = linelist[13] # 手机在网时长
                    phone_consume_info = linelist[14] # 手机月均消费金额
                    lottery_trade = linelist[15] # 12个月博彩金额
                    trade_avg_money = linelist[16] # 12个月平均消费金额
                    trade_avg_month_count = linelist[17] # 平均消费月数
                    channel_ufenqi = linelist[18].strip() # 优分期渠道
                    if channel_ufenqi == '':
                        channel_ufenqi = 0
                    elif channel_ufenqi == 'A':
                        channel_ufenqi = 1
                    elif channel_ufenqi == 'B':
                        channel_ufenqi = 2
                    elif channel_ufenqi == 'C':
                        channel_ufenqi = 3
                    jd_brdz = linelist[19].strip() # 京东申请人本人地址变化情况
                    jd_gzdz = linelist[20] # 京东申请人工作地址变化情况
                    jd_is_real_name = linelist[21]
                    jd_account_use_time = linelist[22] # 京东账号使用时长
                    jd_baitiao_score = linelist[23]
                    jd_baitiao_amount = linelist[24]
                    jd_account_grade = linelist[25].strip()
                    if jd_account_grade == '':
                        jd_account_grade = 0
                    elif jd_account_grade == '注册会员':
                        jd_account_grade = 1
                    elif jd_account_grade == '铜牌会员':
                        jd_account_grade = 2
                    elif jd_account_grade == '银牌会员':
                        jd_account_grade = 3
                    elif jd_account_grade == '金牌会员':
                        jd_account_grade = 4
                    elif jd_account_grade == '钻石会员':
                        jd_account_grade = 5
                    jd_12_month_valid_buy_months = linelist[26] # 京东近12个月有效消费月数
                    jd_12_month_avg_buy_num = linelist[27] # 京东近12个月月均消费次数
                    jd_12_month_avg_buy_money = linelist[28] # 京东近12个月月均消费金额
                    jd_avg_buy_money_month = linelist[29] # 近n个月月均消费金额 
                    jd_valid_buy_months = linelist[30] # 近n个月活跃月份
                    jd_avg_buy_num_month = linelist[31] # 近n个月月均消费次数 
                    dep_month_avg_money = linelist[32] # 借记卡月消费金额
                    dep_month_avg_num = linelist[33] # 过去12个月借记卡消费月数
                    re_list = [userid, 
                               odd,
                               status,
                               age,
                               gender,
                               degree,
                               face,
                               contacts_num,
                               sensword_num,
                               name_matched,
                               ic_matched,
                               phone_call_count_sum,
                               phone_call_time_sum,
                               phone_online_time,
                               phone_consume_info,
                               lottery_trade,
                               trade_avg_money,
                               trade_avg_month_count,
                               channel_ufenqi,
                               jd_brdz,
                               jd_gzdz,
                               jd_is_real_name,
                               jd_account_use_time,
                               jd_baitiao_score,
                               jd_baitiao_amount,
                               jd_account_grade,
                               jd_12_month_valid_buy_months,
                               jd_12_month_avg_buy_num,
                               jd_12_month_avg_buy_money,
                               jd_avg_buy_money_month,
                               jd_valid_buy_months,
                               jd_avg_buy_num_month,
                               dep_month_avg_money,
                               dep_month_avg_num]
                    re_list = [str(item) for item in re_list]
                    wfp.write('\t'.join(re_list) + '\n')
                               
                except Exception, e:
                    sys.stderr.write("exp: " + str(e))
        wfp.close()        

    def pro_train_1(self):
        """
        通讯录处理
        """
        pass

    def pro_uniq(self):
        pro_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_basic'
        output_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_1'
        info = {}
        with open(pro_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('\t')
                userid = linelist[0]
                if userid not in info:
                    info[userid] = line
                elif info[userid] == line:
                    pass
                else:
                    sys.stderr.write(info[userid] + line)
        with open(output_file, 'w') as wfp:
            for k in info:
                wfp.write(info[k])
    
    def pro_fea_filter_1(self):
        """
        根据预期天数确定lable, 去除缺失率80%以上的
        预期天数 >=30 为逾期 lable = 1
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_1'
        output_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_1'
        wfp = open(output_file, 'w')
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                linelist = line.split('\t')
                overduedays = int(linelist[1])
                if overduedays >= 30:
                    linelist[1] = '1'
                else:
                    linelist[1] = '0'
                # 去掉status
                linelist = linelist[:2] + linelist[3:15] + linelist[32:]
                wfp.write('\t'.join(linelist))
        wfp.close()
    
    def pro_fea_filter_2(self):
        """
        缺失值填充,未做, 直接 赋予默认值或者取均值
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/b_test/test1'
        output_file = '/home/liuxiaoliang/workspace/feature/working/b_test/test2'
        face_s = 0
        face_c = 0
        phone_1_s = 0 # phone_call_count_sum
        phone_1_c = 0
        phone_2_s = 0 # phone_call_time_sum
        phone_2_c = 0
        phone_3_s = 0 # phone_online_time
        phone_3_c = 0
        phone_4_s =  0 # phone_consume_info
        phone_4_c = 0
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                linelist = line.split('\t')
                linelist[-1] = linelist[-1].strip()
                if linelist[5] != '':
                    face_s += float(linelist[5])
                    face_c += 1
                if linelist[10] != '':
                    phone_1_s += int(linelist[10])
                    phone_1_c += 1
                if linelist[11] != '':
                    phone_2_s += int(linelist[11])
                    phone_2_c += 1
                if linelist[12] != '':
                    phone_3_s += int(linelist[12])
                    phone_3_c += 1
                if linelist[13] != '':
                    phone_4_s += int(linelist[13])
                    phone_4_c += 1
        face_insert_value = str(face_s/face_c)
        with open(pro_file, 'r') as rfp, open(output_file, 'w') as wfp:
            for line in rfp:
                linelist = line.split('\t')
                linelist[-1] = linelist[-1].strip()
                if linelist[5] == '':
                    linelist[5] = face_insert_value
                if linelist[8] == '':
                    linelist[8] = '-1'
                if linelist[9] == '':
                    linelist[9] = '-1'
                if linelist[10] == '':
                    linelist[10] = str(int(float(phone_1_s)/phone_1_c))
                if linelist[11] == '':
                    linelist[11] = str(int(float(phone_2_s)/phone_2_c))
                if linelist[12] == '':
                    linelist[12] = str(int(float(phone_3_s)/phone_3_c))
                if linelist[13] == '':
                    linelist[13] = str(int(float(phone_4_s)/phone_4_c))
                #linelist = linelist[:-2]
                wfp.write('\t'.join(linelist) + '\n')
        
    def pro_fea_filter_3(self):
        """
        分桶: 按阶段简单划分
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/b_test/test2'
        output_file = '/home/liuxiaoliang/workspace/feature/working/b_test/test3'
        with open(pro_file, 'r') as rfp, open(output_file, 'w') as wfp:
            for line in rfp:
                linelist = line.strip().split('\t')
                # 2 age
                # 5 face
                # 6 contacts_num
                # 10 phone_call_count_sum
                # 11 phone_call_time_sum
                # 12 phone_online_time
                # 13 phone_consume_info
                age = 0
                face = 0
                contacts_num = 0
                phone_1 = 0
                phone_2 = 0
                phone_3 = 0
                phone_4 = 0
                try:
                    age = int(linelist[2])
                    face = float(linelist[5])
                    contacts_num = int(linelist[6])
                    phone_1 = int(linelist[10])
                    phone_2 = int(linelist[11])
                    phone_3 = int(linelist[12])
                    phone_4 = int(linelist[13])
                except Exception, e:
                    sys.stderr.write('exp: ' + str(e) + '\n')
                    print linelist
                    continue
                if (age >= 0 and age < 22): age = 0
                elif (age >= 22 and age < 27): age = 1
                elif (age >= 27 and age < 32): age = 2
                elif (age >= 32 and age < 38): age = 3
                elif (age >= 38 and age < 41): age = 4
                elif (age >= 41): age = 5
                linelist[2] = str(age)
                if (face >= 0 and face < 70): face = 0
                elif (face >= 70 and face < 80): face = 1
                elif (face >= 80 and face < 83): face = 2
                elif (face >= 83 and face < 86): face = 3
                elif (face >= 86 and face < 90): face = 4
                elif (face >= 90): face = 5
                linelist[5] = str(face)
                if (contacts_num >= 0 and contacts_num < 50): contacts_num = 0
                elif (contacts_num >= 50 and contacts_num < 80): contacts_num = 1
                elif (contacts_num >= 80 and contacts_num < 100): contacts_num = 2
                elif (contacts_num >= 100): contacts_num = 3
                linelist[6] = str(contacts_num)
                if (phone_1 >= 0 and phone_1 < 51): phone_1 = 0
                elif (phone_1 >= 51 and phone_1 < 483): phone_1 = 1
                elif (phone_1 >= 483 and phone_1 < 960): phone_1 = 2
                elif (phone_1 >= 960 and phone_1 < 2574): phone_1 = 3
                elif (phone_1 >= 2574 and phone_1 < 4521): phone_1 = 4
                elif (phone_1 >= 4521): phone_1 = 5
                linelist[10] = str(phone_1)
                if (phone_2 >= 0 and phone_2 < 101): phone_2 = 0
                elif (phone_2 >= 100 and phone_2 < 501): phone_2 = 1
                elif (phone_2 >= 501 and phone_2 < 1001): phone_2 = 2
                elif (phone_2 >= 1001 and phone_2 <= 2001): phone_2 = 3
                elif (phone_2 >= 2001 and phone_2 <= 4001): phone_2 = 4
                elif (phone_2 >= 4001 and phone_2 <= 6001): phone_2 = 5
                elif (phone_2 >= 6001 and phone_2 <= 8001): phone_2 = 7
                elif (phone_2 >= 8001): phone_2 = 8
                linelist[11] = str(phone_2)
                if (phone_3 >= 0 and phone_3 < 31): phone_3 = 0
                elif (phone_3 >= 31 and phone_3 < 51): phone_3 = 1
                elif (phone_3 >= 51 and phone_3 < 71): phone_3 = 2
                elif (phone_3 >= 71 and phone_3 < 101): phone_3 = 3
                elif (phone_3 >= 101 and phone_3 < 141): phone_3 = 4
                elif (phone_3 >= 141): phone_3 = 5
                linelist[12] = str(phone_3)
                if (phone_4 >= 0 and phone_4 < 51): phone_4 = 0
                elif (phone_4 >= 51 and phone_4 < 101): phone_4 = 1
                elif (phone_4 >= 101 and phone_4 < 151): phone_4 = 2
                elif (phone_4 >= 151 and phone_4 < 201): phone_4 = 3
                elif (phone_4 >= 201 and phone_4 < 251): phone_4 = 4
                elif (phone_4 >= 251 and phone_4 < 301): phone_4 = 5
                elif (phone_4 >= 301): phone_4 = 6
                linelist[13] = str(phone_4)
                wfp.write('\t'.join(linelist) + '\n')
    
    def pro_fea_filter_3_1(self):
        """
        算法分桶
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk/train_1'
        bin_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1/train_filter_2_bin_vals'
        output_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk/train_2'
        bin_info = {}
        with open(bin_file, 'r') as rfp:
            bin_info = json.loads(rfp.readline().strip())
        index_keys = [int(i) for i in bin_info.keys()]
        #print index_keys
        #print bin_info
        with open(pro_file, 'r') as rfp, open(output_file, 'w') as wfp:
            for line in rfp:
                linelist = line.strip().split('\t')
                new_linelist = linelist[:]
                for i, v in enumerate(linelist):
                    if i in index_keys:
                        bins = bin_info[str(i)]
                        nv = 0
                        for b in bins:
                            if float(v) <= float(b):
                                break
                            nv += 1
                        new_linelist[i] = str(nv)
                wfp.write('\t'.join(new_linelist) + '\n')
                
    def pro_fea_filter_3_bin_1(self):
        """
        分桶: 信息增益
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_2'
        output_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_3_bin_vals'
        y = []
        age = [] # 2
        face = [] # 5
        contacts_num = [] # 6
        phone_call_count_sum = [] # 10
        phone_call_time_sum = [] # 11
        phone_online_time = [] # 12
        phone_consume_info = [] # 13
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                linelist = line.strip().split('\t')
                y.append(int(linelist[1]))
                age.append(float(linelist[2]))
                face.append(float(linelist[5]))
                contacts_num.append(float(linelist[6]))
                phone_call_count_sum.append(float(linelist[10]))
                phone_call_time_sum.append(float(linelist[11]))
                phone_online_time.append(float(linelist[12]))
                phone_consume_info.append(float(linelist[13]))
        age_bucket = self.split(age, y)
        face_bucket = self.split(face, y)
        print age_bucket
    
    def pro_fea_filter_3_bin_2(self):
        """
        分桶: 卡方
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_2'
        output_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_2_bin_vals'
        y = []
        age = [] # 2 
        face = [] # 5
        contacts_num = [] # 6
        phone_call_count_sum = [] # 10
        phone_call_time_sum = [] # 11
        phone_online_time = [] # 12
        phone_consume_info = [] # 13
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                linelist = line.strip().split('\t')
                y.append(int(linelist[1]))
                age.append(float(linelist[2]))
                face.append(float(linelist[5]))
                contacts_num.append(float(linelist[6]))
                phone_call_count_sum.append(float(linelist[10]))
                phone_call_time_sum.append(float(linelist[11]))
                phone_online_time.append(float(linelist[12]))
                phone_consume_info.append(float(linelist[13]))
        age_bin = self.split_chi(age, y, 5)
        face_bin = self.split_chi(face, y, 5)
        contacts_num_bin = self.split_chi(contacts_num, y, 5)
        phone_call_count_sum_bin = self.split_chi(phone_call_count_sum, y, 4)
        phone_call_time_sum_bin = self.split_chi(phone_call_time_sum, y, 4)
        phone_online_time_bin = self.split_chi(phone_online_time, y, 4)
        phone_consume_info_bin = self.split_chi(phone_consume_info, y, 4)
        bins = {2: age_bin,
                5: face_bin,
                6: contacts_num_bin,
                10: phone_call_count_sum_bin,
                11: phone_call_time_sum_bin,
                12: phone_online_time_bin,
                13: phone_consume_info_bin}
        with open(output_file, 'w') as wfp:
            wfp.write(json.dumps(bins) + '\n')
    def pro_fea_filter_3_bin_3(self):
        """
        分桶: 均分
        """
        pass
    def pro_fea_one_hot_encode(self):
        pass
    
    def pro_fea_filter_3_3(self):
        """
        打印每一个特征下每个特征值对应的正负样本数
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_3'
        output_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_3_2'
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                pass
    
    def pro_fea_filter_4(self):
        """
        woe
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1/train_2'
        output_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1/train_3'
        woe_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1/woe'
        # 计算每个特征的每个特征值的woe
        _min = -20
        _max = 20
        s = []
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                s.append(line.strip().split('\t'))
        fea_list = []
        woe_list = []
        for i in range(19):
            fea_list.append({})
            woe_list.append({})
        all_count = [0, 0]
        for one in s:
            l = one[1]
            if l == '1':
                all_count[1] += 1
            elif l == '0':
                all_count[0] += 1
            for fi, f in enumerate(one[2:]):
                if f not in fea_list[fi]:
                    fea_list[fi][f] = [0, 0]
                if l == '1':
                    fea_list[fi][f][1] += 1
                elif l == '0':
                    fea_list[fi][f][0] += 1
        for i in range(len(fea_list)):
            for fk, fv in fea_list[i].items():
                if fk not in woe_list[i]:
                    woe_list[i][fk] = 0
                rate_1 = 1.0*fv[1] / all_count[1]
                rate_0 = 1.0*fv[0] / all_count[0]
                iwoe = 0
                if rate_1 == 0:
                    iwoe = _min
                elif rate_0 == 0:
                    iwoe = _max
                else:
                    iwoe = math.log(rate_1/rate_0)
                woe_list[i][fk] = iwoe
        woe = json.dumps(woe_list)
        with open(woe_file, 'w') as fp:
            fp.write(woe+'\n')
        # 替换原始数据的特征值
        with open(output_file, 'w') as wfp:
            for one in s:
                for i in range(19):
                    one[i+2] = str(woe_list[i][one[i+2]])
                #print one
                wfp.write('\t'.join(one) + '\n')
    
    def pro_fea_filter_5(self):
        """
        去线性相关
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_4'
        output_file = '/home/liuxiaoliang/workspace/feature/working/basic_auth/train_filter_5'
        lable = []
        data = []
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                linelist = line.strip().split('\t')
                lable.append(linelist[:2])
                data.append([float(i) for i in linelist[2:]])
        pca=PCA(n_components='mle') 
        new_data=pca.fit_transform(data)
        with open(output_file, 'w') as wfp:
            for i in range(new_data.shape[0]):
                d = [str(j) for j in new_data[i]]
                l = lable[i]
                wfp.write('\t'.join(l+d) + '\n')
        
    def pro_fea4svm(self):
        pro_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_lbl_bin_ohe'
        output_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_lbl_bin_ohe_svm'
        with open(pro_file, 'r') as rfp, open(output_file, 'w') as wfp:
            for line in rfp:
                i = 1
                linelist = line.split('\t')
                for j in range(2, len(linelist)):
                    linelist[j] = str(i) + ':' + linelist[j]
                    i += 1
                wfp.write('\t'.join(linelist))
        
    def pro_fea_split(self, k):
        """
        蓄水池算法, k为百分比
        """
        if k > 100: exit(0)
        pro_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/feature/fea_30_bin_ohe_svm'
        output_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/algo/lr'
        s_1 = output_dir + '/test'
        s_2 = output_dir + '/train'
        s = []
        re = []
        with open(pro_file, 'r') as rfp:
            for line in rfp:
                s.append(line)
        k = int(0.01*k*len(s))
        re = s[:k]
        for i, e in enumerate(s[k:]):
            r = np.random.randint(0,k+i+1)
            if r <= k - 1:
                re [r] = e
        with open(s_1, 'w') as wfp1, open(s_2, 'w') as wfp2:
            for l in s:
                if l in re:
                    wfp1.write(l)
                else:
                    wfp2.write(l)
    
    def cal_roc(self):
        """
        Input: real predict 1-score 具体哪一列是1-score看输出
               1:为逾期
        Output: score, tn(0, 0), fp(0, 1), fn(1, 0), tp(1, 1), fpr, tpr
        """
        cal_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/algo/lr/eval/all_eval'
        output_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/algo/lr/eval/all_roc'
        step = 0.05
        _min = 1
        _max = 0
        eval_data = []
        re_list = []
        with open(cal_file, 'r') as rfp, open(output_file, 'w') as wfp:
            for line in rfp:
                linelist = line.strip().split(' ')
                r = int(linelist[0])
                p = int(linelist[1])
                s = float(linelist[3])
                if s < _min: _min = s
                if s > _max: _max = s
                eval_data.append([r, p, s])
            threshold = _min + step
            while (threshold < _max):
                tn = 0
                fp = 0
                fn = 0
                tp = 0
                for item in eval_data:
                    if item[2] >= threshold:
                        if item[0] == 1: 
                            tp += 1
                        else:
                            fp += 1
                    else:
                        if item[0] == 1:
                            fn += 1
                        else:
                            tn += 1
                # cal
                fpr = 1.0*fp/(fp+tn)
                tpr = 1.0*tp/(tp+fn)
                re = [threshold, tn, fp, fn, tp, fpr, tpr]
                re = [str(i) for i in re]
                wfp.write('\t'.join(re) + '\n')
                threshold += step
    
    def cal_auc(self):
        """
        Input: real predict 1-score  具体哪一列是1-score看输出
               1:为逾期
        Output: auc
        """
        cal_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/algo/lr/eval/all_eval'
        output_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/algo/lr/eval/all_auc'
        eval_data = []
        _M = 0
        _N = 0
        _p = 0
        with open(cal_file, 'r') as rfp:
            for line in rfp:
                linelist = line.strip().split(' ')
                linelist[3] = float(linelist[3])
                eval_data.append(linelist)
        n = len(eval_data)
        eval_data.sort(key=lambda x:x[3], reverse = True)
        #print eval_data
        for e in eval_data:
            if e[0] == '1':
                _M += 1
                _p += n
            else:
                _N += 1
            n -= 1
        auc = 1.0 * (_p-_M*(1+_M)/2)/(_M*_N)
        with open(output_file, 'w') as wfp:
            wfp.write(str(auc) + '\n')
        
    def cal_black_test(self):
        """业务评估
        通过率和逾期率计算
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/online_svm/10/roc'
        output_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/online_svm/10/b_roc'
        with open(pro_file, 'r') as rfp, open(output_file, 'w') as wfp:
            for line in rfp:
                linelist = line.strip().split('\t')
                score = float(linelist[0])
                tn = int(linelist[1])
                fp = int(linelist[2])
                fn = int(linelist[3])
                tp = int(linelist[4])
                pass_rate = 1.0 * (fn+tn)/(tp+fn+fp+tn)
                over_rate = 1.0 * fn/(fn+tn)
                re = [score, pass_rate, over_rate]
                re = [str(i) for i in re]
                wfp.write('\t'.join(re) + '\n')
    
    def cal_ks(self):
        """
        Input: uid score lable
        """
        step_num = 20
        pro_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test/pre_1/predict'
        re_list = []
        with open(pro_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('\t')
                u, s, o, l = linelist
                s = float(s)
                l = int(l)
                o = int(o)
                re_list.append([u, s, o])
        re_list.sort(key = lambda x:x[1])
        rec = len(re_list)
        bad_all = len([i for i in re_list if i[2] >= 10])
        good_all = rec - bad_all
        step = int(1.0*rec/step_num)
        good = 0
        bad = 0
        ks = 0
        for i in range(0, rec, step):
            for j in range(i, i+step):
                if j < rec:
                    if re_list[j][2] >= 10:
                        bad += 1
                    else:
                        good += 1
                else:
                    break
            cur_ks = abs(1.0*good/good_all - 1.0*bad/bad_all)
            if cur_ks > ks:
                ks = cur_ks
            print i, cur_ks
        print 'ks', ks
        
    def cal_gini(self):
        pass
        
    def split_ig (self, x, y, min_interval = 1):
        """
        信息增益分桶
        x: 一维特征, list[float]
        y: lable, int
        """
        _min_epos = 0.66
        final_bin = []
        _min_interval = min_interval  
        min_val = min(x)  
        bin_dict = {}  
        bin_li = []  
        for i in range(len(x)):  
            pos = (x[i] - min_val)/_min_interval * _min_interval  + min_val  
            target = y[i]  
            bin_dict.setdefault(pos,[0,0])             
            if target == 1:  
                bin_dict[pos][0] += 1                  
            else:  
                bin_dict[pos][1] += 1  
          
        for key ,val in bin_dict.iteritems():  
            t = [key]  
            t.extend(val)  
            bin_li.append(t)  
          
        bin_li.sort(cmp=None, key=lambda x : x[0], reverse=False)  
        #print bin_li  
              
        L_index = 0   
        R_index = 1  
        final_bin.append(bin_li[L_index][0])  
        while True:             
            L = bin_li[L_index]              
            R = bin_li[R_index]  
            # using infomation gain;  
            p1 =  L[1]/ (L[1] + L[2] + 0.0)  
            p0 =  L[2]/ (L[1] + L[2] + 0.0)  
              
            if p1 <= 1e-5 or p0 <= 1e-5:  
                LGain = 0   
            else:  
                LGain = -p1*math.log(p1) - p0 * math.log(p0)  
              
            p1 =  R[1]/ (R[1] + R[2] + 0.0)  
            p0 =  R[2]/ (R[1] + R[2] + 0.0)  
            if p1 <= 1e-5 or p0 <= 1e-5:  
                RGain = 0   
            else:  
                RGain = -p1*math.log(p1) - p0 * math.log(p0)  
              
            p1 = (L[1] + R[1])/ (L[1] + L[2] + R[1] + R[2] + 0.0)  
            p0 = (L[2] + R[2])/ (L[1] + L[2] + R[1] + R[2] + 0.0)  
              
            if p1 <= 1e-5 or p0 <= 1e-5:  
                ALLGain = 0   
            else:  
                ALLGain = -p1*math.log(p1) - p0 * math.log(p0)  
            #print np.absolute(ALLGain - LGain - RGain)
            if np.absolute(ALLGain - LGain - RGain) <= _min_epos:  
                # concat the interval;  
                bin_li[L_index][1] += R[1]  
                bin_li[L_index][2] += R[2]  
                R_index += 1  
            else:                  
                L_index = R_index  
                R_index = L_index + 1  
                final_bin.append(bin_li[L_index][0])  
              
            if R_index >= len(bin_li):  
                break  
        return final_bin
    
    def split_chi(self, x, y, max_interval):
        def chi2(A):  
            '''
            Compute the Chi-Square value 
            '''     
            #print A
            m=len(A);  
            k=len(A[0])  
            R=[]  
            for i in range(m):  
                sum=0  
                for j in range(k):  
                    sum+=A[i][j]  
                R.append(sum)  
            #print R
            C=[]  
            for j in range(k):  
                sum=0  
                for i in range(m):  
                    sum+=A[i][j]  
                C.append(sum)  
            #print C
            N=0  
            for ele in C:  
                N+=ele  
            res=0  
            for i in range(m):  
                for j in range(k):  
                    Eij=R[i]*C[j]/N  
                    if Eij!=0:  
                        res=res+(A[i][j]-Eij)**2/Eij  
            return res
        def combine(a, b):
            c=a[:] # c[0]=a[0]  
            for i in range(len(a[1])):  
                c[1][i]+=b[1][i]  
            return c 
        # main
        fea = zip(x, y)
        fea = [list(i) for i in fea]
        fea.sort()
        #print fea
        # count [[xi, yi, cnt], ...]
        fea_cnt = []
        i = 0
        while (i < len(fea)):
            cnt = fea.count(fea[i])
            r = fea[i][:]
            r.append(cnt)
            fea_cnt.append(r)
            i += cnt
        #print fea_cnt
        # 分类统计 {xi:[y1_cnt, y2_cnt], ...}
        fea_dic = {}
        for r in fea_cnt:
            fea_dic.setdefault(r[0], [0, 0])
            if r[1] == 0:
                fea_dic[r[0]][0] += r[2]
            elif r[1] == 1:
                fea_dic[r[0]][1] += r[2]
        fea_tuple = sorted(fea_dic.items())
        #print fea_tuple
        # chimerge
        num_interval=len(fea_tuple)
        while (num_interval > max_interval):
            num_pair = num_interval-1  
            chi_values=[]  
            for i in range(num_pair):  
                arr=[fea_tuple[i][1],fea_tuple[i+1][1]] 
                chi_values.append(chi2(arr))
            min_chi=min(chi_values)
            for i in range(num_pair-1,-1,-1):
                if chi_values[i]==min_chi:
                    fea_tuple[i]=combine(fea_tuple[i],fea_tuple[i+1])
                    fea_tuple[i+1] = 'done'
            while('done' in fea_tuple):
                fea_tuple.remove('done')
            num_interval=len(fea_tuple)
        split_points=[r[0] for r in fea_tuple]
        return split_points
    
    def pca_fit(self):
        pass
        
    def get_q_p(self):
        """
        计算p,q: 用的坏好比,因为lable＝1为欺诈用户
        100 = q - plog9
        110 = q- plog9/2
        """
        A = [[1, -1*np.log(9)], [1, -1*np.log(4.5)]]
        B = [100, 110]
        s = np.linalg.solve(A, B)
        return s[0], s[1]

    def cal_score(self):
        """
        对于新的用户,计算其得分
        """
        pro_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk/test_2' # 分箱后的数据
        woe_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1/woe'
        modle_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1/lr/train.model'
        output_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk/test_predict'
        q, p = self.get_q_p()
        woe_list = []
        with open(woe_file, 'r') as fp:
            woe_list = json.loads(fp.readline().strip())
        #print woe_list
        coe = []
        with open(modle_file, 'r') as fp:
            w_beg_flag = 0
            for line in fp:
                if line.find('bias') >= 0:
                    coe.append(int(line.strip().split(' ')[1].strip()))
                elif w_beg_flag:
                    coe.append(float(line.strip()))
                elif line.strip() == 'w':
                    w_beg_flag = 1
                else:
                    continue
        #print coe
        with open(pro_file, 'r') as fp, open(output_file, 'w') as wfp:
            for line in fp:
                linelist = line.strip().split('\t')
                uid = linelist[0]
                fea = linelist[2:]
                score = q + p*coe[0]
                for i, f in enumerate(fea):
                    fwoe = woe_list[i][f]
                    score += p*coe[i+1]*fwoe
                #print int(score)
                wfp.write(uid + '\t' + str(int(score)) + '\n')
if __name__ == '__main__':
    fe = FeaEtl()
    #fe.pro_lable(30)
    #fe.pro_fea_1()
    #fe.pro_train()
    #fe.pro_uniq()
    #fe.pro_fea_filter_1()
    #fe.pro_fea_filter_2()
    #fe.pro_fea_filter_3()
    #fe.pro_fea_filter_3_1()
    #fe.pro_fea_filter_3_bin_1()
    #fe.pro_fea_filter_3_bin_2()
    #fe.pro_fea_filter_4() 
    #fe.pro_fea_filter_5()
    fe.pro_fea4svm()
    #fe.pro_fea_split(30)
    #fe.cal_roc()
    #fe.cal_auc()
    #fe.cal_black_test()
    #print fe.get_q_p()
    #fe.cal_score()
    #fe.cal_ks()
