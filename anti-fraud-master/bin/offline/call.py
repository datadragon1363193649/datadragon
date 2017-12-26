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
import traceback

class CallInfo(object):
    """
    process call details info
    """
    def __init__(self):
        pass
        
    def e(self):
        pro_dir = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk/detail/10'
        output_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk/fea/10'
        black_file = '/home/liuxiaoliang/workspace/feature/working/call_data/phonenum/black'
        pass_file = '/home/liuxiaoliang/workspace/feature/working/call_data/phonenum/pass'
        refuse_file = '/home/liuxiaoliang/workspace/feature/working/call_data/phonenum/refuse'
        b_list = set()
        p_list = set()
        r_list = set()
        with open(black_file, 'r') as fp:
            for line in fp:
                b_list.add(line.strip())
        with open(pass_file, 'r') as fp:
            for line in fp:
                p_list.add(line.strip())
        with open(refuse_file, 'r') as fp:
            for line in fp:
                r_list.add(line.strip())
        p_file = os.listdir(pro_dir)
        wfp = open(output_file, 'w')
        for p in p_file:
            uid = p
            # 重要联系人
            importent_call_num = 0 # 重要联系人联系次数
            # 有名频繁联系人
            name_freq_call_count = 0 # 有名频繁人数
            name_freq_calling_num = 0 # 有名频繁呼出次数
            name_freq_called_num = 0 # 有名频繁呼入次数
            name_freq_calling_time = 0 # 有名频繁呼出时间
            name_freq_called_time = 0 # 有名频繁呼入时间
            # 有名非频繁联系人
            name_call_count = 0 # 有名非频繁人数
            name_calling_num = 0 # 有名非频繁呼出次数
            name_called_num = 0 # 有名非频繁呼入次数
            name_calling_time = 0 # 有名非频繁呼出时间
            name_called_time = 0 # 有名非频繁呼入时间
            # 无名联系人
            no_name_call_count = 0 # 无名人数
            no_name_calling_num = 0 # 无名呼出次数
            no_name_called_num = 0 # 无名呼入次数
            no_name_calling_time = 0 # 无名呼出时间
            no_name_called_time = 0 # 无名呼入时间
            # 1度关系
            black_count_1 = 0 # 1度黑名单人数
            pass_count_1 = 0 # 1度系统认证pass人数
            refuse_count_1 = 0 # 1度系统认证refuse人数
            # 联系人
            contacts = set() 
            with open(pro_dir + '/' + p, 'r') as fp:
                c = fp.readline()
                try:
                    c = json.loads(c)
                except Exception, e:
                    sys.stderr.write('exp: ' + str(e) + '\n')
                    continue
                for i in range(len(c)):
                    if i == 0:
                        # 本人
                        # 开卡时长 
                        continue
                    elif i == 1 or i == 2:
                        # 重点联系人
                        importent_call_num += int(c[i]['callNum'])
                        contacts.add(c[i]['phone'])
                    else:
                        contacts.add(c[i]['phone'])
                        first_call_time = c[i]['firstCallTime']
                        last_call_time = c[i]['lastCallTime']
                        call_days = 0
                        if first_call_time != '' and last_call_time != '':
                            fd = datetime.datetime.strptime(first_call_time, "%Y-%m-%d %H:%M:%S")
                            ld = datetime.datetime.strptime(last_call_time, "%Y-%m-%d %H:%M:%S")
                            call_days = (ld-fd).days
                        call_weeks = int(round(1.0*call_days/7))
                        call_num = c[i]['callNum']
                        called_num = c[i]['calledNum']
                        calling_num = c[i]['callingNum']
                        called_time = c[i]['calledTime']
                        calling_time = c[i]['callingTime']
                        if c[i]['name'] != '':
                            # 有名   
                            if call_num >= 10:
                                # 频繁 一段时间call次数多或者单次时间长
                                name_freq_call_count += 1
                                name_freq_calling_num += calling_num
                                name_freq_called_num += called_num
                                name_freq_calling_time += calling_time
                                name_freq_called_time += called_time
                            else:
                                # 非频繁
                                name_call_count += 1
                                name_calling_num += calling_num
                                name_called_num += called_num
                                name_calling_time += calling_time
                                name_called_time += called_time
                        else:
                            # 无名
                            no_name_call_count += 1
                            no_name_calling_num += calling_num
                            no_name_called_num += called_num
                            no_name_calling_time += calling_time
                            no_name_called_time += called_time
            # 1度黑名单人数
            black_count_1 = len(contacts & b_list)
            # 1度认证pass
            pass_count_1 = len(contacts & p_list)
            # 1度认证refuse
            refuse_count_1 = len(contacts & r_list)
            re = [
                uid,
                importent_call_num,
                name_freq_call_count,
                1.0*name_freq_calling_num/(name_freq_call_count+1),
                1.0*name_freq_called_num/(name_freq_call_count+1),
                1.0*name_freq_calling_time/(name_freq_call_count+1),
                1.0*name_freq_called_time/(name_freq_call_count+1),
                name_call_count,
                1.0*name_calling_num/(name_call_count+1),
                1.0*name_called_num/(name_call_count+1),
                1.0*name_calling_time/(name_call_count+1),
                1.0*name_called_time/(name_call_count+1),
                no_name_call_count,
                1.0*no_name_calling_num/(no_name_call_count+1),
                1.0*no_name_called_num/(no_name_call_count+1),
                1.0*no_name_calling_time/(no_name_call_count+1),
                1.0*no_name_called_time/(no_name_call_count+1), 
                black_count_1,
                pass_count_1,
                refuse_count_1]
            re = [str(i) for i in re]
            wfp.write('\t'.join(re)+'\n')
    
    def p(self):
        """
        处理黑名单／系统拒绝／系统通过三种用户的通讯录
        只添加有名的通讯录
        """
        t_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/refuse'
        c_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/contacts/20160601_20161114/all'
        output_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/refuse_contacts'
        p = {}
        with open(t_file, 'r') as fp:
            for line in fp:
                uid, phn = line.strip().split('\t')
                p[uid] = phn
        c_dir_list = os.listdir(c_dir)
        wfp = open(output_file, 'w')
        for d in p:
            if d in c_dir_list:
                cs = ''
                with open(c_dir + '/' + d, 'r') as tfp:
                    cs = tfp.readline()
                    try:
                        cs = json.loads(cs)
                    except Exception, e:
                        sys.stderr.write('exp: ' + str(e) + '\n')
                        continue
                if cs == '':
                    continue
                c_list = []
                for pd in cs:
                    try:
                        pp = pd['phone']
                        pn = pd['name'].strip()
                        if pn == '':
                            continue
                        for pi in pp:
                            pi = pi.replace(' ', '').replace('+86', '').replace('-', '')
                            if len(pi) == 13 and pi[:2] == '86':
                                pi = pi[2:]
                            c_list.append(pi.encode('utf-8'))
                    except:
                        continue
                try:
                    wfp.write(d + '\t' + '\t'.join(c_list) + '\n')
                except:
                    print c_list
                    exit(0)
        wfp.close()
                
    def e2(self):
        # 样本
        s_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/sample/s'
        # 联系人
        c_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/contacts'
        # 通讯详情
        d_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/details'
        output_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_2'
        # black pass refuse
        b_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/black'
        p_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/pass'
        pc_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/pass_contacts'
        r_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/refuse'
        rc_dir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/refuse_contacts'
        black = set()
        pss = set()
        rfs = set()
        pss_contact = {}
        rfs_contact = {}
        with open(b_dir, 'r') as fp:
            tmplist = []
            for line in fp:
                tmplist.append(line.strip())
            black = set(tmplist)
        with open(p_dir, 'r') as fp:
            tmplist = []
            for line in fp:
                tmplist.append(line.strip().split('\t')[1])
            pss = set(tmplist)
        with open(r_dir, 'r') as fp:
            tmplist = []
            for line in fp:
                tmplist.append(line.strip().split('\t')[1])
            rfs = set(tmplist)
        with open(pc_dir, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('\t')
                pss_contact[linelist[0]] = set(linelist[1:])
        with open(rc_dir, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('\t')
                rfs_contact[linelist[0]] = set(linelist[1:])
        uid_dic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('\t')
                uid_dic[linelist[0]] = 0
        c_files = os.listdir(c_dir)
        d_files = os.listdir(d_dir)
        c_dic = {}
        d_dic = {}
        for i in range(len(c_files)):
            c_dic[c_files[i]] = 0
        for i in range(len(d_files)):
            d_dic[d_files[i]] = 0
        open_time = 0 # 开卡时长
        locate_remote_person_radio = 0 # 本地／异地通讯总联系人数比例
        locate_remote_num_radio = 0 # 本地／异地通讯总联系次数比例
        call_time_segment_0 = 0 # 通话时间段变化情况 早上
        call_time_segment_1 = 0 # 下午
        call_time_segment_2 = 0 # 晚上
        call_time_length = 0 # 通话长度变化情况
        call_time_frequency_day = 0 # 通话频率变化情况 一天最长通话次数
        call_time_frequency_week = 0 # 通话频率变化情况 平均每周通话次数
        cantact_person_diff_last_month = 0 # 最近一个月联系人变化情况
        live_contact_person = 0 # 活跃联系人比例
        contact_name = 0 # 通讯录名字分布情况
        contact_name_sensitive_count = 0 # 含敏感词通讯录个数
        contact_name_sensitive_call_num = 0 # 含敏感词通讯录最近三个月通话次数
        contact_name_family_count = 0 # 含亲属词通讯录
        contact_name_family_call_num = 0
        contact_name_job_count = 0 # 含职务类词通讯录
        contact_name_job_call_num = 0
        contact_name_server_count = 0 # 含服务类词通讯录
        contact_name_server_call_num = 0
        name_freq_call_count = 0 # 有名频繁人数
        name_freq_callnum = 0 # 有名频繁联系次数
        name_freq_call_time = 0 # 有名频繁联系时长
        name_call_count = 0 # 有名非频繁人数
        name_call_num = 0 # 有名非频繁联系次数
        name_call_time = 0 # 有名非频繁联系时长
        no_name_call_count = 0 # 无名人数
        no_name_call_num = 0 # 无名联系次数
        no_name_call_time = 0 # 无名联系时长
        wfp = open(output_file, 'w')
        for u in uid_dic:
            #u = '885052' # debug
            black_count_1 = 0 # 1度黑名单人数
            pass_count_1 = 0 # 1度系统认证pass人数
            refuse_count_1 = 0 # 1度系统认证refuse人数
            pass_count_2_1 = 0 # 最近一个月联系人在历史用户的通讯录出现的历史用户个数
            pass_count_2_2 = 0 # 最近一个月联系人在历史用户通讯录出现的总个数／联系人在历史用户的通讯录出现的历史用户个数
            refuse_count_2_1 = 0
            refuse_count_2_2 = 0
            if (u in c_dic) and (u in d_dic):
                ud = ''
                uc = ''
                with open(d_dir + '/' + u, 'r') as fp:
                    ud = fp.readline()
                with open(c_dir + '/' + u, 'r') as fp:
                    uc = fp.readline()
                try:
                    ud = json.loads(ud)
                    uc = json.loads(uc)
                    ud_content = [] # (开卡时间, [phone, time, time_segment, leangth, locate])
                    if 'code' in ud:
                        # wecash
                        ud_content = self.e2_wecash(ud)
                    else:
                        # lhp
                        # 以最大城市作为本地
                        ud_content = self.e2_lhp(ud)
                    open_time = ud_content[0]
                    ud_call_list = ud_content[1]
                    uc_list = self.e2_contact(uc)
                    fea1, contact_phn_stat = self.e2_fea(ud_call_list, uc_list) # contact_phn_stat {'phn':[call_num, call_time_length],...}
                    contact_phn = set(contact_phn_stat.keys())
                    # 1 度
                    black_count_1 = len(contact_phn & black)
                    pass_count_1 = len(contact_phn & pss)
                    refuse_count_1 = len(contact_phn & rfs)
                    # 2 度
                    for k in pss_contact:
                        a = contact_phn & pss_contact[k]
                        if len(a) > 0:
                            pass_count_2_1 += 1
                            pass_count_2_2 += len(a)
                    pass_count_2_2 = 1.0*pass_count_2_2/(pass_count_2_1+1)
                    for k in rfs_contact:
                        a = contact_phn & rfs_contact[k]
                        if len(a) > 0:
                            refuse_count_2_1 += 1
                            refuse_count_2_2 += len(a)
                    refuse_count_2_2 = 1.0*refuse_count_2_2/(refuse_count_2_1+1)
                    fea = [open_time] + \
                          fea1 + \
                          [black_count_1,
                           pass_count_1,
                           refuse_count_1,
                           pass_count_2_1,
                           pass_count_2_2,
                           refuse_count_2_1,
                           refuse_count_2_2]
                    fea = [str(i) for i in fea]
                    wfp.write(u + '\t' + '\t'.join(fea) + '\n')
                except Exception, e:
                    sys.stderr.write('exp: ' + str(e) + '\t' + u + '\n')
                    print 'exec: ',  traceback.print_exc()
                    #break
                    continue
            #break
        wfp.close()
    def e2_wecash(self, ud):
        open_time = ''
        ophn = ''
        if 'innetDate' in ud['data']['transportation'][0]['origin']['baseInfo']['data']:
            open_time = ud['data']['transportation'][0]['origin']['baseInfo']['data']['innetDate'] # 20141230
        if 'contactNum' in ud['data']['transportation'][0]['origin']['baseInfo']['data']:
            ophn = ud['data']['transportation'][0]['origin']['baseInfo']['data']['contactNum']
        callinfo = ud['data']['transportation'][0]['origin']['callInfo']['data'] # []
        if open_time == '':
            open_time = '20160101'
        open_time = datetime.datetime.strptime(open_time, "%Y%m%d")
        cur_time = datetime.datetime.now()
        open_time_length = int(round(1.0*(cur_time - open_time).days/30))
        call_list = [] # 最近三个月
        for c in callinfo:
            if c['code'] != 'E000000':
                continue
            month = c['month']
            clist = []
            for cd in c['details']:
                try:
                    phn = cd['anotherNm']
                    st = cd['startTime'] # 2016-09-27 16:40:03
                    if st == '':
                        st = '2016-11-27 9:40:03'
                    elif st.find('-') == 2:
                        st = '2016-' + st
                    elif st.find('-') > 0 and st.find('/') > 0:
                        st = st.split('-')[1]
                    elif len(st.split('-')) == 4:
                        st = '-'.join(st.split('-')[1:])
                        
                    time_segment = 0 # 上午 0; 下午 1; 晚上 2
                    hour = 1
                    if st.find('-') > 0:
                        hour = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S").hour
                    elif st.find('/') > 0:
                        hour = datetime.datetime.strptime(st, "%Y/%m/%d %H:%M:%S").hour
                    if hour >= 7 and hour <= 12:
                        time_segment = 0
                    elif hour > 12 and hour <= 18:
                        time_segment = 1
                    else:
                        time_segment = 2
                    cl = u"2分13秒"
                    if 'commTime' in cd:
                        cl = cd['commTime'] # 3时2分13秒
                    h = 0
                    m = 0
                    s = 0
                    if cl.find('时'.decode('utf-8')) > 0 and cl.find('小'.decode('utf-8')) > 0:
                        h = int(cl[cl.find('小'.decode('utf-8'))-1])
                    elif cl.find('时'.decode('utf-8')) > 0:
                        h = int(cl[cl.find('时'.decode('utf-8'))-1])
                    if cl.find('分'.decode('utf-8')) > 0:
                        m = int(cl[cl.find('分'.decode('utf-8'))-1])
                    if cl.find('秒'.decode('utf-8')) > 0:
                        s = int(cl[cl.find('秒'.decode('utf-8'))-1])
                    cl = h*60 + m + int(round(1.0*s/60))
                    cc = 0
                    ca = u"本地"
                    if 'commType' in cd:
                        ca = cd['commType'] # 本地 其他
                    if ca.find("本地".decode('utf-8')) >=0 or ca.find("市话".decode('utf-8')) >= 0 or ca.find("国内".decode('utf-8')) >= 0:
                        cc = 0
                    else:
                        cc = 1
                    clist.append([phn, st, time_segment, cl, cc])
                except Exception, e:
                    sys.stderr.write('exp: ' + str(e) + '\n')
                    print 'exec: ',  traceback.print_exc()
                    print 'cd:' , cd
                    print 'phone: ', ophn
            clist.sort(key=lambda x:x[1], reverse=True)
            call_list.append((month, clist))
        call_list.sort(key=lambda x:x[0], reverse=True)
        if len(call_list) > 3:
            call_list = call_list[:3]
        call_list = [item[1] for item in call_list] # remove month
        call_list = reduce(lambda x,y:x+y, call_list)
        return (open_time_length, call_list)
        
    def e2_lhp(self, ud):
        open_time = ud['phoneList'][0]['registerDate']
        callinfo = ud['phoneList'][0]['telData']
        ophn = ud['phoneList'][0]['phone']
        if open_time == '':
            open_time = 'Wed Sep 24 21:56:38 CST 2016'
        open_time = datetime.datetime.strptime(open_time, "%a %b %d %H:%M:%S %Z %Y")
        cur_time = datetime.datetime.now()
        open_time_length = int(round(1.0*(cur_time - open_time).days/30))
        call_list = [] # 最近三个月
        city = {}
        for c in callinfo:
            try:
                phn = c['receiverPhone']
                st = c['cTime'] # 1464748326000
                if st == '':
                    st = '2016-11-27 9:40:03'
                else:
                    st = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(st)/1000))
                time_segment = 0 # 上午 0; 下午 1; 晚上 2
                hour = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S").hour
                if hour >= 7 and hour <= 12:
                    time_segment = 0
                elif hour > 12 and hour <= 18:
                    time_segment = 1
                else:
                    time_segment = 2
                cl = c['tradeTime'] # second
                cl = int(round(float(cl)/60))
                city.setdefault(c['tradeAddr'], 0)
                city[c['tradeAddr']] += 1
                # 如果有tradeType就不用city
                ct = c['tradeAddr']
                cc = -1
                if 'tradeType' in c:
                    cp = c['tradeType']
                    if cp.find('本地'.decode('utf-8')) >= 0:
                        cc = 0
                    else:
                        cc = 1
                if cc == -1:
                    cc = ct
                call_list.append([phn, st, time_segment, cl, cc])
            except Exception, e:
                sys.stderr.write("exe: " + str(e) + '\n')
                print 'exec: ',  traceback.print_exc()
                print 'phone: ', ophn
        max_num_city = ''
        city = sorted(city.items(), key = lambda x:x[1], reverse = True)
        max_num_city = city[0]
        new_call_list = []
        for c in call_list:
            if c[4] == 0 or c[4] == 1:
                new_call_list.append(c)
                continue
            if c[4] == max_num_city: # 本地
                c[4] = 0
            else:
                c[4] = 1
            new_call_list.append(c)
        
        new_call_list = call_list
        new_call_list.sort(key = lambda x:x[1], reverse = True)
        anchor = (datetime.datetime.strptime(new_call_list[0][1], "%Y-%m-%d %H:%M:%S") 
                  - datetime.timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")
        # 取最近三个月
        new_call_list = [i for i in new_call_list if i[1] >= anchor]
        return (open_time_length, new_call_list)

    def e2_contact(self, uc):
        #通讯录名字长度从到小排列  有名 无名  敏感词 亲属词 服务词 职务词 
        family_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/contacts_name/family'
        family_words = []
        job_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/contacts_name/job'
        job_words = []
        server_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/contacts_name/server'
        server_words = []
        with open(family_file, 'r') as f, open(job_file, 'r') as j, open(server_file, 'r') as s:
            for line in f:
                if line.strip() != '':
                    family_words.append(line.strip().decode('utf-8'))
            for line in j:
                if line.strip() != '':
                    job_words.append(line.strip().decode('utf-8'))
            for line in s:
                if line.strip() != '':
                    server_words.append(line.strip().decode('utf-8'))
        sensitive_words = [u'金融理财',u'借款', u'贷款', u'放款', u'急用钱',
                           u'分期', u'推销', u'骚扰', u'传销', u'淘宝刷单',
                           u'套现', u'信用卡']
        family_words_str = ' '.join(family_words)
        job_words_str = ' '.join(job_words)
        server_words_str = ' '.join(server_words)
        sensitive_words_str = ' '.join(sensitive_words)
        contact_name = []
        contact_name_phn = []
        sensitive_phn = []
        family_phn = []
        job_phn = []
        server_phn = []
        all_count = 0
        name_phn = {}
        for c in uc:
            try:
                n = c['name']
                plist = c['phone']
                new_plist = []
                for p in plist:
                    p = p.replace(' ', '').replace('+86', '').replace('-', '')
                    if len(p) == 13 and p[:2] == '86':
                        p = p[2:]
                    new_plist.append(p.encode('utf-8'))
                if n.strip() == '':
                    continue
                contact_name.append(n)
                contact_name_phn += new_plist
                name_phn[n] = new_plist
                all_count += len(new_plist)
            except Exception, e:
                sys.stderr.write('exc1 ' + str(e) + '\n')
                continue
        for n in contact_name:
            for w in sensitive_words:
                if n.find(w) >= 0 or w.find(n) >= 0:
                    sensitive_phn += name_phn[n]
            for w in family_words:
                if n.find(w) >= 0 or w.find(n) >= 0:
                    family_phn += name_phn[n]
            for w in job_words:
                if n.find(w) >= 0 or w.find(n) >= 0:
                    job_phn += name_phn[n]
            for w in server_words:
                if n.find(w) >= 0 or w.find(n) >= 0:
                    server_phn += name_phn[n]
        contact_name.sort(key=lambda x:len(x))
        return (contact_name, sensitive_phn, family_phn, job_phn, server_phn, all_count, contact_name_phn)
                    
        
    def e2_fea(self, call_list, contact_list):
        # 除网络关系特征以外的特征
        locate_remote_person = [0, 0]
        locate_remote_person_radio = 0
        loacate_remote_num = [0, 0]
        locate_remote_num_radio = 0
        call_time_segment_old = [0, 0, 0] # 早 中 晚
        call_time_segment_new = [0, 0, 0]
        call_time_segment = [0, 0, 0] 
        call_time_length_old = [0, 0] # 通话长度总和  通话次数
        call_time_length_new = [0, 0]
        call_time_length = 0
        call_time_frequency_new = {} # 两个月所有每天的通话次数
        call_time_frequency_old = {}
        call_time_frequency_day = 0
        call_time_frequency_week = 0
        cantact_person = [set(), set()] # 两个月联系人集合
        live_contact_person = 0 # 最近三个月联系人联系次数两次以上
        contact_name = contact_list[0] # 通讯录名字长度从到小排列
        contact_name_stat = 0
        contact_name_sensitive = 0 # 敏感词通讯录 通话次数
        sensitive_phn = contact_list[1]
        contact_name_family = 0
        family_phn = contact_list[2]
        contact_name_job = 0
        job_phn = contact_list[3]
        contact_name_server = 0
        server_phn = contact_list[4]
        contact_phn_count = contact_list[5]
        contact_name_phn = contact_list[6]
        phone_call_stat = {} # 最近三个月[phn, call_num, call_time_length]
        contact_name_length = [len(i) for i in contact_name]
        contact_name_length = list(set(contact_name_length))
        tar_length = 0
        if len(contact_name_length) > 3:
            tar_length = contact_name_length[2]
        else:
            tar_length = contact_name_length[0]
        contact_name_stat_1 = len([i for i in contact_name if len(i) <= tar_length])
        contact_name_stat = 1.0*contact_name_stat_1/len(contact_name)
        anchor1 = (datetime.datetime.strptime(call_list[0][1], "%Y-%m-%d %H:%M:%S") 
                  - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        anchor2 = (datetime.datetime.strptime(call_list[0][1], "%Y-%m-%d %H:%M:%S")
                   - datetime.timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
        for c in call_list:
            #print c
            #break
            phn, st, ts, cl, cc = c
            if phn not in phone_call_stat.keys():
                if cc == 0: # 本地
                    locate_remote_person[0] += 1
                else:
                    locate_remote_person[1] += 1
            if cc == 0:
                loacate_remote_num[0] += 1
            else:
                loacate_remote_num[1] += 1
            phone_call_stat.setdefault(phn, [0, 0])
            phone_call_stat[phn][0] += 1
            phone_call_stat[phn][1] += cl
            day = st.split(' ')[0]
            if st > anchor1: # 最近一个月
                call_time_segment_new[ts] += 1
                call_time_length_new[0] += cl
                call_time_length_new[1] += 1
                call_time_frequency_new.setdefault(day, 0)
                call_time_frequency_new[day] += 1
                cantact_person[0].add(phn)
            elif st > anchor2: # 最近2个月
                call_time_segment_old[ts] += 1
                call_time_length_old[0] += cl
                call_time_length_old[1] += 1
                call_time_frequency_old.setdefault(day, 0)
                call_time_frequency_old[day] += 1
                cantact_person[1].add(phn)
            if phn in sensitive_phn:
                contact_name_sensitive += 1
            elif phn in family_phn:
                contact_name_family += 1
            elif phn in job_phn:
                contact_name_job += 1
            elif phn in server_phn:
                contact_name_server += 1
        call_time_frequency_new = call_time_frequency_new.values()
        call_time_frequency_old = call_time_frequency_old.values()
        call_time_frequency_new.append(0) #避免为空
        call_time_frequency_old.append(0) # 避免为空
        live_contact_person = []
        # 有名频繁 有名非频繁 无名
        name_freq_call_count = 0 # 有名频繁人数
        name_freq_callnum = 0 # 有名频繁联系次数
        name_freq_call_time = 0 # 有名频繁联系时长
        name_call_count = 0 # 有名非频繁人数
        name_call_num = 0 # 有名非频繁联系次数
        name_call_time = 0 # 有名非频繁联系时长
        no_name_call_count = 0 # 无名人数
        no_name_call_num = 0 # 无名联系次数
        no_name_call_time = 0 # 无名联系时长
        for k, v in phone_call_stat.items():
            if v[0] > 10:
                live_contact_person.append(k)
            if k in contact_name_phn: # name
                if v[0] > 10:
                    name_freq_call_count += 1
                    name_freq_callnum += v[0]
                    name_freq_call_time += v[1]
                else:
                    name_call_count += 1
                    name_call_num += v[0]
                    name_call_time += v[1]
            else:
                no_name_call_count += 1
                no_name_call_num += v[0]
                no_name_call_time += v[1]

        fea_list = [
            len(contact_name_phn), # 有名联系人总个数
            1.0*locate_remote_person[0]/(locate_remote_person[1]+1), # locate_remote_person_radio
            1.0*loacate_remote_num[0]/(loacate_remote_num[1]+1), # locate_remote_num_radio
            sum(call_time_segment_new) - sum(call_time_segment_old), # call_time_segment_all
            call_time_segment_new[0] - call_time_segment_new[1], # call_time_segment_new_0
            call_time_segment_new[1] - call_time_segment_new[2], # call_time_segment_new_1
            call_time_segment_old[0] - call_time_segment_old[1], # call_time_segment_old_0
            call_time_segment_old[1] - call_time_segment_old[2], # call_time_segment_old_1
            (1.0*call_time_length_new[0]/(call_time_length_new[1]+1) -
             1.0*call_time_length_old[0]/(call_time_length_old[1]+1)), # call_time_length
            max(call_time_frequency_new) - max(call_time_frequency_old), # call_time_frequency_day
            sum(call_time_frequency_new)/4.0 - sum(call_time_frequency_old)/4.0, # call_time_frequency_week
            len(cantact_person[0] - cantact_person[1]), # cantact_person_diff_last_month
            1.0*len(live_contact_person)/contact_phn_count, # live_contact_person
            contact_name_stat, # contact_name
            len(sensitive_phn), # contact_name_sensitive_count
            contact_name_sensitive, # contact_name_sensitive_call_num
            len(family_phn), # contact_name_family_count
            contact_name_family, # contact_name_family_call_num
            len(job_phn), # contact_name_job_count
            contact_name_job, # contact_name_job_call_num
            len(server_phn), # contact_name_server_count
            contact_name_server, # contact_name_server_call_num
            name_freq_call_count,
            name_freq_callnum,
            name_freq_call_time,
            name_call_count,
            name_call_num,
            name_call_time,
            no_name_call_count,
            no_name_call_num,
            no_name_call_time
        ]
        return (fea_list, phone_call_stat)
            
if __name__ == '__main__':
    c = CallInfo()
    c.e2()
