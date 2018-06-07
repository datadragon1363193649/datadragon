#! /usr/bin/env python
# -*- encoding: utf-8 -*-
import traceback
import json
import pymongo as pm
import time
import datetime
import pandas as pd
import offline_db_conf as dconf


class phn_lay_dot:
    def __init__(self):
        self.mhost1 = dconf.mg_hosty1
        # self.mhost2 = dconf.mg_hosty2
        self.mreplicat_set = dconf.mg_replicat_set
        self.mgdb = dconf.mg_dby
        self.mgcollection = dconf.mg_collection
        self.mgcollectionrelation = dconf.mg_coll_relation
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.mgconn = None
        self.init_mg_conn()
        self.phn_type = dconf.phn_type
        self.goods_list = dconf.goods_list

    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost1])
        self.mgconn[dconf.mg_dby].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self):
        mc = dconf.mg_collection
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]

    def get_click_num(self,stime,login_time,login_bool
                      ,risk_push_time,risk_push_bool
                      ,confirm_order_time,confirm_order_bool
                      ,confirm_pay_time,confirm_pay_bool):
        # 认证的时候获取
        if login_bool ==1 and risk_push_bool ==1:
            if stime > login_time and stime < risk_push_time:
                dconf.login_to_risk_num += 1

        # 贷中的时候获取
        if risk_push_bool ==1 and confirm_order_bool == 1:
            if stime > risk_push_time and stime < confirm_order_time:
                dconf.risk_to_order_num += 1
        if confirm_order_bool == 1 and confirm_pay_bool == 1:
            if stime > confirm_order_time and stime < confirm_pay_time:
                dconf.order_to_pay_num += 1

    def get_feature(self,file_name):
        c = self.get_mg_conn()
        in_file = dconf.data_path + file_name
        out_file = dconf.data_path + 'fea_name_d'
        wfp = open(out_file,'w')
        out_file_1 = dconf.data_path + 'feature_d'
        wfp1 = open(out_file_1, 'w')
        feature_name_dic = {}
        timei=0
        with open(in_file,'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                # linelist = ['h_19487']
                # print linelist[0]
                try:
                    re = c.find({'userIdentity': linelist[1]})
                    re_list = list(re)
                except Exception, e:
                    print e
                    continue
                    # print re_list
                ti = 0
                if len(re_list)==0:
                    continue
                hide_dic = {}
                show_dic = {}
                type_dic = {}
                behavior_num_dic = {}
                login_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                login_bool = 0
                risk_push_time = '1990-01-01 00:00:00'
                risk_push_bool = 0
                confirm_order_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                confirm_order_bool = 0
                confirm_pay_time = '1990-01-01 00:00:00'
                confirm_pay_bool = 0
                for user in re_list:
                    # print user
                    try:
                        if user['type'] == 'onshow':
                            page_dic = {}
                            page_dic['fromPage'] = user['fromPage']
                            page_dic['toPage'] = user['toPage']
                            ctime = datetime.datetime.strptime(user['time'], '%b %d, %Y %I:%M:%S %p'
                                                               ).strftime('%Y-%m-%d %H:%M:%S')
                            show_dic[ctime] = page_dic
                        elif user['type'] == 'onhide':
                            page_dic = {}
                            page_dic['fromPage'] = user['fromPage']
                            page_dic['toPage'] = user['toPage']
                            ctime = datetime.datetime.strptime(user['time'], '%b %d, %Y %I:%M:%S %p'
                                                               ).strftime('%Y-%m-%d %H:%M:%S')
                            hide_dic[ctime] = page_dic
                        elif user['type'] in self.phn_type:
                        # if user['type'] in self.phn_type:
                            ctime = datetime.datetime.strptime(user['time'], '%b %d, %Y %I:%M:%S %p'
                                                               ).strftime('%Y-%m-%d %H:%M:%S')
                            type_dic[ctime] = user['type']
                            if user['type'] == 'login' and ctime < login_time:
                                login_time = ctime
                                login_bool = 1
                            if user['type'] == 'risk_push' and ctime > risk_push_time:
                                risk_push_time = ctime
                                risk_push_bool = 1
                            if user['type'] == 'confirm_order' and ctime < confirm_order_time:
                                confirm_order_time = ctime
                                confirm_order_bool = 1
                            if user['type'] == 'confirm_pay' and ctime > confirm_pay_time:
                                confirm_pay_time = ctime
                                confirm_pay_bool = 1
                        elif self.goods_list in user['type']:
                            ctime = datetime.datetime.strptime(user['time'], '%b %d, %Y %I:%M:%S %p'
                                                               ).strftime('%Y-%m-%d %H:%M:%S')
                            type_dic[ctime] = self.goods_list
                        else:
                           ti += 1
                    except Exception, e:
                        print e
                # print show_dic
                dconf.login_to_risk_num = 0
                dconf.risk_to_order_num = 0
                dconf.order_to_pay_num = 0
                # show_list = [[k,show_dic[k]] for k in sorted(show_dic.keys())]
                # print show_list
                # print hide_dic
                for sl in show_dic:
                    if sl in hide_dic:
                        hide_dic.pop(sl)
                    name=show_dic[sl]['fromPage'] + '_to_' + show_dic[sl]['toPage']
                    behavior_num_dic.setdefault(name,0)
                    behavior_num_dic[name] += 1
                    self.get_click_num(sl,login_time,login_bool
                                       ,risk_push_time,risk_push_bool
                                       ,confirm_order_time,confirm_order_bool
                                       ,confirm_pay_time,confirm_pay_bool)
                for sl in hide_dic:
                    name=hide_dic[sl]['fromPage'] + '_to_' + hide_dic[sl]['toPage']+'_hide'
                    behavior_num_dic.setdefault(name,0)
                    behavior_num_dic[name] += 1
                    self.get_click_num(sl,login_time,login_bool
                                       ,risk_push_time,risk_push_bool
                                       ,confirm_order_time,confirm_order_bool
                                       ,confirm_pay_time,confirm_pay_bool)
                for sl in type_dic:
                    name=type_dic[sl]
                    behavior_num_dic.setdefault(name,0)
                    behavior_num_dic[name] += 1
                    self.get_click_num(sl,login_time,login_bool
                                       ,risk_push_time,risk_push_bool
                                       ,confirm_order_time,confirm_order_bool
                                       ,confirm_pay_time,confirm_pay_bool)
                behavior_num_dic['login_to_risk_num'] = dconf.login_to_risk_num
                behavior_num_dic['risk_to_order_num'] = dconf.risk_to_order_num
                behavior_num_dic['order_to_pay_num'] = dconf.order_to_pay_num

                dconf.login_to_risk_duration = -1
                dconf.risk_to_order_duration = -1
                dconf.order_to_pay_duration = -1
                risk_push_time_p = datetime.datetime.strptime(risk_push_time, "%Y-%m-%d %H:%M:%S")
                login_time_p = datetime.datetime.strptime(login_time, "%Y-%m-%d %H:%M:%S")
                confirm_order_time_p = datetime.datetime.strptime(confirm_order_time, "%Y-%m-%d %H:%M:%S")
                confirm_pay_time_p = datetime.datetime.strptime(confirm_pay_time, "%Y-%m-%d %H:%M:%S")
                if risk_push_time > login_time and login_bool ==1 and risk_push_bool ==1:
                    dconf.login_to_risk_duration = int(round(
                        1.0 * (risk_push_time_p - login_time_p).total_seconds() / 60))

                if confirm_order_time > risk_push_time and risk_push_bool ==1 \
                        and confirm_order_bool == 1:
                    dconf.risk_to_order_duration = int(round(
                        1.0 * (confirm_order_time_p - risk_push_time_p).total_seconds() / 60))
                if confirm_pay_time > confirm_order_time and confirm_order_bool == 1 \
                        and confirm_pay_bool == 1:
                    dconf.order_to_pay_duration = int(round(
                        1.0 * (confirm_pay_time_p - confirm_pay_time_p).total_seconds() / 60))
                behavior_num_dic['login_to_risk_duration'] = dconf.login_to_risk_duration
                behavior_num_dic['risk_to_order_duration'] = dconf.risk_to_order_duration
                behavior_num_dic['order_to_pay_duration'] = dconf.order_to_pay_duration

                for bnd in behavior_num_dic:
                    out_list=[linelist[1],bnd,str(behavior_num_dic[bnd])]
                    wfp1.write(','.join(out_list) + '\n')
                    feature_name_dic.setdefault(bnd,0)
                if timei%10 == 0:
                    print timei
                    print datetime.datetime.now()
                    time.sleep(0.1)
                timei += 1
                # print len(behavior_num_dic)
                # sum=0
                # for k in behavior_num_dic:
                #     sum += behavior_num_dic[k]
                #     print k, behavior_num_dic[k]
                # print sum
                # print ti
                # break
        for key in feature_name_dic:
            wfp.write(key+'\n')
        wfp.close()
        wfp1.close()

    def feature_df(self):
        in_file_1 = dconf.data_path + 'fea_name_d'
        in_file_2 = dconf.data_path + 'feature_d'
        out_file = dconf.data_path + 'feature_d.csv'
        name_list=[]
        with open(in_file_1,'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                name_list.append(linelist[0])
        userid = '0'
        feature_list = []
        f_dic = {}
        f_list = []

        with open(in_file_2,'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                if linelist[0] != userid:
                    if len(f_dic) == 0:
                        userid = linelist[0]
                        f_dic[linelist[1]] = linelist[2]
                        continue
                    f_list.append(userid)
                    for nl in name_list:
                        if nl in f_dic:
                            f_list.append(f_dic[nl])
                        else:
                            f_list.append('.')
                    feature_list.append(f_list)
                    f_dic = {}
                    f_list = []
                    userid = linelist[0]
                    f_dic[linelist[1]] = linelist[2]
                else:
                    f_dic[linelist[1]] = linelist[2]
        name1_list = ['userid'] + name_list
        tdf = pd.DataFrame(feature_list,columns= name1_list)
        tdf.to_csv(out_file,index=False)

    def test(self):
        in_file_1 = dconf.data_path + 'fea_name'
        name_dic = {}
        with open(in_file_1, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('_to_')
                name_dic[linelist[0]]=0
                if 'hide' in linelist[1]:
                    continue
                name_dic[linelist[1]] = 0
        for nd in name_dic:
            print nd
    def test1(self):
        in_file_1 = dconf.data_path + 'fea_name'
        name_dic = {}
        with open(in_file_1, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('_to_')
                if linelist[0] == linelist[1]:
                    print line
if __name__ == '__main__':
    pld=phn_lay_dot()
    pld.get_feature('merchant_id')
    pld.feature_df()
    # pld.test1()