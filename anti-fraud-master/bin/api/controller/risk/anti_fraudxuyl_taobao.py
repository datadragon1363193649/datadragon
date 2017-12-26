#! /usr/bin/env python
# -*- encoding: utf-8 -*-

import sys
import os
import multiprocessing

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath = os.path.split(os.path.realpath(_abs_path))[0]
apath = os.path.split(os.path.realpath(apath))[0]
apath = os.path.split(os.path.realpath(apath))[0]
apath = os.path.split(os.path.realpath(apath))[0]
# print apath
sys.path.append(apath)
# import web
import math
from bin.api.model.risk.user_info import CardId, CreditTask, UnioznpayTask, BehaviorTask, RiskUser, Source_type, Contact
# from http_exp import HttpJsonError
from lib.tools.aam import *
import config.common_conf as conf
import config.db_conf_501 as dconf
import datetime
import json
import pymongo as pm
import re
from lib.common.jklogger import JKLOG
import threading
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from numpy import *
from math import *
# encoding=utf8
import datetime
import sys
import time

reload(sys)
sys.setdefaultencoding('utf8')


class AllfeatureFind(object):
    """
    很长一段时间内兼容所有的风控:
      简单借款app
      简单借款h5
      简单借款复购
      优分期app
      优分期复购
    """

    def __init__(self):
        # mongodb
        # self.mhost1 = dconf.mg_host1
        self.mhost2 = dconf.mg_host2
        self.mreplicat_set = dconf.mg_replicat_set
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.conn = None
        self.connlbs = None
        self.init_mg_conn()
        self.init_mg_connlbs()
        self.conn_old = None

        self.mhosty1 = dconf.mg_hosty1
        self.conny = None
        self.init_mg_conn_yellowpage()
        # self.init_mg_conn_old()
        # mysql
        self.db_engine = create_engine(dconf.DB_USER_BASIC_INFO, encoding='utf-8', pool_recycle=30, poolclass=NullPool)
        # self.db_sudaibear = create_engine(dconf.DB_USER_BASIC_INFO_SUDAIBEAR, encoding='utf-8', pool_recycle=30,
        #                                   poolclass=NullPool)
        self.Session = sessionmaker(bind=self.db_engine)
        # self.Sessionsudai = sessionmaker(bind=self.db_sudaibear)
        # fea
        self.fea_schm_file = dconf.fea_schm_file
        self.feature_schema = {}
        self.load_fea_schm()
        self.feature = []
        # print '11111', self.feature_schema
        # model
        self.jk_app_model_dir = dconf.risk_jk_app_model
        self.jk_h5_model_dir = dconf.risk_jk_h5_model
        self.jk_app_model = {}
        self.jk_h5_model = {}
        self.jk_app_bins = {}
        self.jk_h5_bins = {}
        self.load_model_all()
        # print '22222', self.jk_app_model
        # print '33333', self.jk_h5_model
        # print '44444', self.jk_app_bins
        # print  '5555', self.jk_h5_bins
        # get dict data
        self.aam = UAAMachine()
        self.load_dict_contacts_name()
        self.aam.build_automation()

        self.bank_yellowpage = {}
        self.collection_yellowpage = {}
        self.bank_operator_list=dconf.bank_operator_list
        self.mg_wecash_taobao_report=dconf.mg_wecash_taobao_report
        self.operator_name_dic=dconf.operator_name_dic
        self.member_type_dic=dconf.member_type_dic

    def init_mg_conn(self):
        # self.conn = pm.MongoClient([self.mhost2],replicaSet=self.mreplicat_set, maxPoolSize = 10)
        self.conn = pm.MongoClient([self.mhost2], maxPoolSize=10)
        self.conn[dconf.mg_db].authenticate(self.muname, self.mpasswd)
        try:
            self.conn.server_info()
        except Exception, e:
            JKLOG.Error([('e', 'mongodb error'), ('exp type', str(e))])
            # raise HttpJsonError(610, 'DB Error')

    def init_mg_connlbs(self):
        # self.conn = pm.MongoClient([self.mhost2],replicaSet=self.mreplicat_set, maxPoolSize = 10)
        self.connlbs = pm.MongoClient([dconf.mg_hostlbs2], maxPoolSize=10)
        self.connlbs[dconf.mg_db].authenticate(self.muname, self.mpasswd)
        try:
            self.conn.server_info()
        except Exception, e:
            JKLOG.Error([('e', 'mongodb error'), ('exp type', str(e))])

    def init_mg_conn_yellowpage(self):
        # self.conn = pm.MongoClient([self.mhost2],replicaSet=self.mreplicat_set, maxPoolSize = 10)
        self.conny = pm.MongoClient([self.mhosty1], maxPoolSize=10)
        # self.conn[dconf.mg_db].authenticate(self.muname, self.mpasswd)
        try:
            self.conny.server_info()
        except Exception, e:
            JKLOG.Error([('e', 'mongodb error'), ('exp type', str(e))])

    def init_mg_conn_old(self):
        self.conn_old = pm.MongoClient([dconf.o_mg_host1, dconf.o_mg_host2], replicaSet=dconf.o_mg_replicat_set,
                                       maxPoolSize=10)
        self.conn_old[dconf.o_mg_db].authenticate(dconf.o_mg_uname, dconf.o_mg_passwd)
        try:
            self.conn_old.server_info()
        except Exception, e:
            JKLOG.Error([('e', 'mongodb error'), ('exp type', str(e))])
            # raise HttpJsonError(610, 'DB Error')

    def get_mg_conn(self, mc):
        mdb = dconf.mg_db
        # mc = dconf.mg_collection
        # if mdb not in self.conn.database_names():
        #    JKLOG.Error([('e', 'mongodb error'), ('exp type', 'no this db %s'%mdb)])
        #    raise HttpJsonError(610, 'DB Error')
        mdb = self.conn[dconf.mg_db]
        # if mc not in mdb.collection_names():
        #    JKLOG.Error([('e', 'mongodb error'), ('exp type', 'no this collection %s'%mc)])
        #    raise HttpJsonError(610, 'DB Error')
        return mdb[mc]

    def get_mg_connlbs(self, mc):
        mdb = dconf.mg_db
        # mc = dconf.mg_collectionlbs
        mdb = self.connlbs[dconf.mg_db]
        return mdb[mc]

    def get_mg_conn_yellow(self, mc):
        mdb = dconf.mg_dby
        # mc = dconf.mg_collection
        # if mdb not in self.conn.database_names():
        #    JKLOG.Error([('e', 'mongodb error'), ('exp type', 'no this db %s'%mdb)])
        #    raise HttpJsonError(610, 'DB Error')
        mdb = self.conny[dconf.mg_dby]
        # if mc not in mdb.collection_names():
        #    JKLOG.Error([('e', 'mongodb error'), ('exp type', 'no this collection %s'%mc)])
        #    raise HttpJsonError(610, 'DB Error')
        return mdb[mc]

    def get_mg_conn_old(self, mc):
        mdb = dconf.o_mg_db
        mdb = self.conn_old[dconf.o_mg_db]
        return mdb[mc]

    def is_phn_exist(self, phn):
        c = self.get_mg_conn(dconf.mg_collection)
        if not c.find_one({'_id': phn}):
            return False
        return True

    def load_fea_schm(self):
        with open(self.fea_schm_file, 'r') as fp:
            for line in fp:
                fid, fn = line.strip().split('\t')
                self.feature_schema[int(fid)] = fn

    def load_model_all(self):
        self.jk_app_model, self.jk_app_bins = self.load_model_one(self.jk_app_model_dir)
        self.jk_h5_model, self.jk_h5_bins = self.load_model_one(self.jk_h5_model_dir)

    def load_model_one(self, md):
        m = {}  # model
        f = {}  # feature
        vlist = os.listdir(md)
        for v in vlist:
            flist = os.listdir(md + '/' + v)
            for i in flist:
                fpath = md + '/' + v + '/' + i
                if i.endswith('.f'):
                    f[int(v)] = self.read_bins(fpath)
                elif i.endswith('.m'):
                    m[int(v)] = self.read_model(fpath)
        return (m, f)

    def load_dict_contacts_name(self):
        f = dconf.risk_contatcs_name
        with open(f, 'r') as fp:
            for line in fp:
                try:
                    w, c = line.strip().split('\t')
                    w = w.decode('utf-8')
                    c = int(c)
                    self.aam.insert(w, c)
                except:
                    continue

    def read_bins(self, bf):
        bins = {}
        with open(bf, 'r') as fp:
            bins = json.loads(fp.readline().strip())
        return bins

    def read_model(self, mfile):
        """
        model format:
        第一行是b,如果b>0,则将b加入feature list最后一个
        其余是w
        """
        m = []
        with open(mfile, 'r') as fp:
            for line in fp:
                w = line.strip()
                if w:
                    m.append(float(w))
        return m

    def get_score(self, phn, app_type, v):
        s = 0
        if app_type == 0:
            # if (v not in self.jk_app_bins) or (v not in self.jk_app_model):
            # raise HttpJsonError(700, 'no this version %s' % v)
            self.calc_feature_jk_app(phn, v)
            s = self.calc_score(self.jk_app_model[v])
        elif app_type == 1:
            # if (v not in self.jk_h5_bins) or (v not in self.jk_h5_model):
            # raise HttpJsonError(700, 'no this version %s' % v)
            self.calc_feature_jk_h5(phn, v)
            s = self.calc_score(self.jk_h5_model[v])
            # else:
            # raise HttpJsonError(601, 'app type dose not exist')
        JKLOG.Info([('x', 'result'), ('phone', phn), ('app type', app_type), ('score', s), ('version', v)])
        return s

    def calc_score(self, model):
        s = 0
        # 标志概率属于1和0
        l = int(model[0])
        # b
        if model[1] > 0:
            self.feature.append(model[1])
        w = model[2:]
        for i in range(len(self.feature)):
            s += 1.0 * self.feature[i] * w[i]
        s = 1.0 / (1 + math.exp(-s))
        if l == 1:
            s = 1 - s
        return s

    def calc_feature_jk_app(self, phn, v):
        self.feature = []
        f1 = self.get_jk_basic_fea(phn)
        self.feature += f1
        f2 = self.get_comm_fea(phn)
        self.feature += f2
        # self.store_feature(phn)
        self.split_feature_ohe(phn, self.jk_app_bins[v])

    def calc_feature_jk_h5(self, phn, v):
        self.feature = []
        f1 = self.get_jk_basic_fea(phn)
        self.feature += f1
        f2 = self.get_comm_fea(phn)
        self.feature += f2
        # self.store_feature(phn)
        self.split_feature_ohe(phn, self.jk_h5_bins[v])
    def before_some_days(self, d, delta):
        return (datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S") -
                datetime.timedelta(days=delta)).strftime("%Y-%m-%d %H:%M:%S")

    def get_taobao_fea(self,phn):
        taobao_open_time=-1
        taobao_recently_interval = -1
        taobao_order_num_6=-1
        taobao_order_average_money_6=-1
        taobao_order_money_6=-1
        taobao_order_num_no_bill_6 = -1
        taobao_order_average_money_no_bill_6 = -1
        taobao_order_money_no_bill_6 = -1
        taobao_order_max_money=-1
        taobao_order_max_num = -1
        taobao_favorite_commodity_num=-1
        taobao_favorite_commodity_money = -1
        taobao_favorite_shop_num = -1
        taobao_paytype_ratio_6=-1
        taobao_addressee_num=-1
        taobao_member_type=-1
        session = self.Session()
        re = None
        try:
            re = session.query(RiskUser.id).filter(
                RiskUser.mobile == phn).first()
        except Exception, e:
            JKLOG.Error([('e', 'get riskid failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print phn,re
        if re is None:
            JKLOG.Info([('e', 'get riskid failue, no info'), ('phone', phn)])
            return taobao_open_time,taobao_recently_interval,taobao_order_num_6,\
                   taobao_order_average_money_6,taobao_order_money_6,taobao_order_num_no_bill_6,\
                    taobao_order_average_money_no_bill_6,taobao_order_money_no_bill_6,\
                    taobao_order_max_money,taobao_order_max_num,taobao_favorite_commodity_num,\
                    taobao_favorite_commodity_money,\
                    taobao_favorite_shop_num,taobao_paytype_ratio_6,taobao_addressee_num,taobao_member_type
        try:
            tbmonconn = self.get_mg_connlbs(dconf.mg_wecash_taobao_report)
            rs = tbmonconn.find({'userId': re.id })
            if rs.count() > 0:
                rsl = list(rs)
                # print rsl[0]['taobaoInfo']
                taobao_info=rsl[0]['taobaoInfo']
                taobao_info=json.loads(taobao_info)
                lately_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                anchor1 = self.before_some_days(lately_time, 180)
                try:
                    lately_time = taobao_info['update_date']
                    # lately_time = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                    anchor1 = self.before_some_days(lately_time, 180)
                except Exception, e:
                    JKLOG.Error([('data', 'update_date failue'), ('error', str(e))])
                # print lately_time,anchor1
                if 'orders' in taobao_info['taobao']:

                    max_order_date='2000-00-00 00:00:00'
                    min_order_date=lately_time
                    # print min_order_date
                    orders=taobao_info['taobao']['orders']
                    taobao_order_num_6=0
                    taobao_order_money_6=0
                    taobao_order_num_no_bill_6=0
                    taobao_order_money_no_bill_6=0
                    paytype_num=0
                    date_num={}
                    date_money={}
                    for ord in orders:
                        # print "111"
                        try:
                            if 'transaction_time' in ord:
                                transaction_time = ord['transaction_time']
                                # print transaction_time
                                if transaction_time < min_order_date:
                                    min_order_date = transaction_time
                            if ord['status']==u'交易成功':
                                if 'transaction_time' in ord:
                                    transaction_time=ord['transaction_time']
                                    if transaction_time>max_order_date:
                                        max_order_date=transaction_time
                                    if transaction_time>anchor1:
                                        taobao_order_num_6+=1
                                        order_price=0
                                        if 'price' in ord:
                                            order_price=float(ord['price'])
                                        # print ord['status'],transaction_time,ord['payType'],order_price
                                        taobao_order_money_6+=order_price
                                        date_num.setdefault(transaction_time[:7], 0)
                                        date_num[transaction_time[:7]] += 1
                                        date_money.setdefault(transaction_time[:7], 0)
                                        date_money[transaction_time[:7]] += order_price
                                        operator_tf=0
                                        if 'payType' in ord:
                                            if ord['payType']!=u'支付宝':
                                                paytype_num+=1
                                        if 'merchants' in ord:
                                            for operator_name in self.operator_name_dic:
                                                if operator_name in ord['merchants']:
                                                    # print ord['merchants']
                                                    operator_tf=1
                                                    break
                                            if operator_tf==0:
                                                taobao_order_num_no_bill_6+=1
                                                taobao_order_money_no_bill_6+=order_price
                        except Exception,e:
                            JKLOG.Error([('data', 'orders failue'), ('error', str(e))])
                    # print date_num
                    # print date_money
                    lately_time = datetime.datetime.strptime(lately_time, "%Y-%m-%d %H:%M:%S")
                    max_order_date = datetime.datetime.strptime(max_order_date, "%Y-%m-%d %H:%M:%S")
                    min_order_date = datetime.datetime.strptime(min_order_date, "%Y-%m-%d %H:%M:%S")
                    taobao_open_time=int(round(1.0 * (lately_time - min_order_date).days / 30))
                    taobao_recently_interval=(lately_time-max_order_date).days
                    taobao_order_average_money_6 = taobao_order_money_6 * 1.0 / (taobao_order_num_6 + 1)
                    taobao_order_average_money_no_bill_6=taobao_order_money_no_bill_6*1.0/\
                                                         (taobao_order_num_no_bill_6+1)
                    taobao_paytype_ratio_6=paytype_num*1.0/(taobao_order_num_6 + 1)
                    date_num = date_num.values() + [0]
                    date_money = date_money.values() + [0]
                    taobao_order_max_num=max(date_num)
                    taobao_order_max_money=max(date_money)
                if 'consignee_info' in taobao_info['taobao']:
                    consignee=taobao_info['taobao']['consignee_info']
                    consignee_name_dic={}
                    for cons in consignee:
                        if 'name' in cons:
                            consignee_name_dic[cons['name']]=0
                    # print consignee_name_dic
                    taobao_addressee_num=len(consignee_name_dic)
                if 'base_info' in taobao_info['taobao']:
                    if 'taobao_level' in taobao_info['taobao']['base_info']:
                        member_type=taobao_info['taobao']['base_info']['taobao_level']
                        if member_type in self.member_type_dic:
                            taobao_member_type=self.member_type_dic[member_type]
                            # print phn,member_type,taobao_member_type
                if 'favorite' in taobao_info['taobao']:
                    favorite=taobao_info['taobao']['favorite']
                    taobao_favorite_commodity_num=len(favorite)
                    taobao_favorite_commodity_money=0
                    for fa in favorite:
                        if 'price' in fa:
                            taobao_favorite_commodity_money+=fa['price']
                    # taobao_favorite_commodity_average_money=taobao_favorite_commodity_money*1.0\
                    #                                         /(taobao_favorite_commodity_num+1)
                if 'favorite_shop' in taobao_info['taobao']:
                    favorite_shop=taobao_info['taobao']['favorite_shop']
                    taobao_favorite_shop_num=len(favorite_shop)
        except Exception, e:
            JKLOG.Error([('e', 'get taobao report failue'), ('exp type', str(e))])
            return taobao_open_time, taobao_recently_interval, taobao_order_num_6, \
                   taobao_order_average_money_6, taobao_order_money_6, taobao_order_num_no_bill_6, \
                   taobao_order_average_money_no_bill_6, taobao_order_money_no_bill_6, \
                   taobao_order_max_money, taobao_order_max_num, taobao_favorite_commodity_num, \
                    taobao_favorite_commodity_money, \
                   taobao_favorite_shop_num, taobao_paytype_ratio_6, taobao_addressee_num, taobao_member_type
        # print taobao_open_time, taobao_recently_interval, taobao_order_num_6, \
        #            taobao_order_average_money_6, taobao_order_money_6, taobao_order_num_no_bill_6, \
        #            taobao_order_average_money_no_bill_6, taobao_order_money_no_bill_6, \
        #            taobao_order_max_money, taobao_order_max_num, taobao_favorite_commodity_num, \
        #             taobao_favorite_commodity_money, \
        #            taobao_favorite_shop_num, taobao_paytype_ratio_6, taobao_addressee_num, taobao_member_type
        return taobao_open_time, taobao_recently_interval, taobao_order_num_6, \
                   taobao_order_average_money_6, taobao_order_money_6, taobao_order_num_no_bill_6, \
                   taobao_order_average_money_no_bill_6, taobao_order_money_no_bill_6, \
                   taobao_order_max_money, taobao_order_max_num, taobao_favorite_commodity_num, \
                   taobao_favorite_commodity_money, \
                   taobao_favorite_shop_num, taobao_paytype_ratio_6, taobao_addressee_num, taobao_member_type
    def get_all_feature(self,file_name):
        cf = self.get_mg_conn_yellow(dconf.mg_coll_fea)
        init_file = '/home/xuyonglong/feature/working/experiment/201705/sample/'+file_name
        out_file='test'
        wfp=open(out_file,'w')
        userdic = {}
        with open(init_file, 'r')as fp:
            for line in fp:
                linelist = line.strip().split(',')
                userdic[linelist[0]] = 0
        num = 0
        # print userdic.keys()[:5]
        for phn in userdic:
            # phn='13611111111'
            rs = cf.find({'_id': phn})
            if rs.count() > 0:
                # print rs['taobao']
                if 'taobao' in rs:
                    if 'taobao_open_time' in rs:
                        continue
            if not rs or rs.count() == 0:
                # continue
                # print rs
                cf.insert({'_id': phn})
                # continue
            taobao_open_time,\
            taobao_recently_interval, \
            taobao_order_num_6,\
            taobao_order_average_money_6,\
            taobao_order_money_6,\
            taobao_order_num_no_bill_6, \
            taobao_order_average_money_no_bill_6,\
            taobao_order_money_no_bill_6, \
            taobao_order_max_money,\
            taobao_order_max_num, \
            taobao_favorite_commodity_num, \
            taobao_favorite_commodity_money, \
            taobao_favorite_shop_num,\
            taobao_paytype_ratio_6, \
            taobao_addressee_num,\
            taobao_member_type=self.get_taobao_fea(phn)
            cf.update({'_id': phn}, {'$set': {'taobao.taobao_open_time': taobao_open_time,
                                              'taobao.taobao_recently_interval': taobao_recently_interval,
                                              'taobao.taobao_order_num_6': taobao_order_num_6,
                                              'taobao.taobao_order_average_money_6': taobao_order_average_money_6,
                                              'taobao.taobao_order_money_6': taobao_order_money_6,
                                              'taobao.taobao_order_num_no_bill_6': taobao_order_num_no_bill_6,
                                              'taobao.taobao_order_average_money_no_bill_6': taobao_order_average_money_no_bill_6,
                                              'taobao.taobao_order_money_no_bill_6': taobao_order_money_no_bill_6,
                                              'taobao.taobao_order_max_money': taobao_order_max_money,
                                              'taobao.taobao_order_max_num': taobao_order_max_num,
                                              'taobao.taobao_favorite_commodity_num': taobao_favorite_commodity_num,
                                              'taobao.taobao_favorite_commodity_money': taobao_favorite_commodity_money,
                                              'taobao.taobao_favorite_shop_num': taobao_favorite_shop_num,
                                              'taobao.taobao_paytype_ratio_6': taobao_paytype_ratio_6,
                                              'taobao.taobao_addressee_num': taobao_addressee_num,
                                              'taobao.taobao_member_type': taobao_member_type
                                              }})

            # gender, age = self.get_jk_basic_fea(phn)
            # # print 'age',age
            # open_time, \
            # contact_sum, \
            # locate_remote_person_ratio, \
            # locate_remote_num_ratio, \
            # call_time_segment_all, \
            # call_time_segment_new_am_pm, \
            # call_time_segment_new_pm_night, \
            # call_time_segment_old_am_pm, \
            # call_time_segment_old_pm_night, \
            # call_time_length, \
            # call_time_frequency_day, \
            # call_time_frequency_week, \
            # cantact_person_diff_last_month, \
            # live_contact_person, \
            # contact_name_distribution, \
            # contact_name_sensitive_count, \
            # contact_name_sensitive_call_num, \
            # contact_name_family_count, \
            # contact_name_family_call_num, \
            # contact_name_job_count, \
            # contact_name_job_call_num, \
            # contact_name_server_count, \
            # contact_name_server_call_num, \
            # name_freq_call_count_27, \
            # name_freq_call_num, \
            # name_freq_call_time, \
            # name_freq_call_count, \
            # name_no_freq_call_count, \
            # name_call_time, \
            # no_name_call_count, \
            # no_name_call_num, \
            # no_name_call_time, \
            # black_count_detail, \
            # pass_count_detail, \
            # refuse_count_detail, \
            # pass_count_recent_count, \
            # pass_count_recent_all_ratio, \
            # refuse_count_all_count, \
            # refuse_count_recent_all_ratio \
            #     = self.get_comm_fea(phn)
            #
            #
            #
            # life_work_distance, \
            # life_auth_distance, \
            # work_auth_distance, \
            # bank_count_contact, \
            # cui_count_contact, \
            # bank_count_detail, \
            # cui_count_detail, \
            # bank_max_count_detail, \
            # cui_max_count_detail \
            #     = self.get_diantance_yellowpage_fea(phn)
            #
            # # 获取手机系统类型
            # st = self.get_source_type(phn)
            # day_no_call = self.get_maxNocom_sourcetype(phn)



            # vip_pass_count, vip_other_vip_count = self.get_contact_feature(phn)
            # # print vip_pass_count, vip_other_vip_count
            # pass_count_refuse_count_ratio = -1
            # if refuse_count_all_count==-1:
            #     pass_count_refuse_count_ratio=-1
            # else:
            #     pass_count_refuse_count_ratio=1.0*pass_count_recent_count/(refuse_count_all_count+1)

            # cf.update({'_id': phn}, {'$set': {'call.gender': gender,
            #                                   'call.age': age,
            #                                   'call.open_time': open_time,
            #                                   'call.contact_sum': contact_sum,
            #                                   'call.locate_remote_person_ratio': locate_remote_person_ratio,
            #                                   'call.locate_remote_num_ratio': locate_remote_num_ratio,
            #                                   'call.call_time_segment_all': call_time_segment_all,
            #                                   'call.call_time_segment_new_am_pm': call_time_segment_new_am_pm,
            #                                   'call.call_time_segment_new_pm_night': call_time_segment_new_pm_night,
            #                                   'call.call_time_segment_old_am_pm': call_time_segment_old_am_pm,
            #                                   'call.call_time_segment_old_pm_night': call_time_segment_old_pm_night,
            #                                   'call.call_time_length': call_time_length,
            #                                   'call.call_time_frequency_day': call_time_frequency_day,
            #                                   'call.call_time_frequency_week': call_time_frequency_week,
            #                                   'call.cantact_person_diff_last_month': cantact_person_diff_last_month,
            #                                   'call.live_contact_person': live_contact_person,
            #                                   'call.contact_name_distribution': contact_name_distribution,
            #                                   'call.contact_name_sensitive_count': contact_name_sensitive_count,
            #                                   'call.contact_name_sensitive_call_num': contact_name_sensitive_call_num,
            #                                   'call.contact_name_family_count': contact_name_family_count,
            #                                   'call.contact_name_family_call_num': contact_name_family_call_num,
            #                                   'call.contact_name_job_count': contact_name_job_count,
            #                                   'call.contact_name_job_call_num': contact_name_job_call_num,
            #                                   'call.contact_name_server_count': contact_name_server_count,
            #                                   'call.contact_name_server_call_num': contact_name_server_call_num,
            #                                   'call.name_freq_call_count_27': name_freq_call_count_27,
            #                                   'call.name_freq_call_num': name_freq_call_num,
            #                                   'call.name_freq_call_time': name_freq_call_time,
            #                                   'call.name_freq_call_count': name_freq_call_count,
            #                                   'call.name_no_freq_call_count': name_no_freq_call_count,
            #                                   'call.name_call_time': name_call_time,
            #                                   'call.no_name_call_count': no_name_call_count,
            #                                   'call.no_name_call_num': no_name_call_num,
            #                                   'call.no_name_call_time': no_name_call_time,
            #                                   'call.black_count_detail': black_count_detail,
            #                                   'call.pass_count_detail': pass_count_detail,
            #                                   'call.refuse_count_detail': refuse_count_detail,
            #                                   'call.pass_count_recent_count': pass_count_recent_count,
            #                                   'call.pass_count_recent_all_ratio': pass_count_recent_all_ratio,
            #                                   'call.refuse_count_all_count': refuse_count_all_count,
            #                                   'call.refuse_count_recent_all_ratio': refuse_count_recent_all_ratio,
            #                                   'call.life_work_distance': life_work_distance,
            #                                   'call.life_auth_distance': life_auth_distance,
            #                                   'call.work_auth_distance': work_auth_distance,
            #                                   'call.bank_count_contact': bank_count_contact,
            #                                   'call.cui_count_contact': cui_count_contact,
            #                                   'call.bank_count_detail': bank_count_detail,
            #                                   'call.cui_count_detail': cui_count_detail,
            #                                   'call.bank_max_count_detail': bank_max_count_detail,
            #                                   'call.cui_max_count_detail': cui_max_count_detail,
            #                                   'call.day_no_call': day_no_call,
            #                                   'call.source_type': st,
            #                                   'call.vip_pass_count': vip_pass_count,
            #                                   'call.vip_other_vip_count': vip_other_vip_count,
            #                                   'call.pass_count_refuse_count_ratio':pass_count_refuse_count_ratio
            #                                   }})

            num += 1
            if num % 500 == 0:
                time.sleep(0.5)
                # break

    def __del__(self):
        self.conn.close()


if __name__ == '__main__':
    a = AllfeatureFind()
    JKLOG.Initialize('111', fname="dltest", log_type=1, jk_log_level="efil",
                     scribe_ip="127.0.0.1", scribe_port=1464, ntrvl=1,
                     backcount=7)
    p1 = multiprocessing.Process(target=a.get_all_feature, args=('ali_score_820_930.txt',))
    # p2 = multiprocessing.Process(target=a.get_all_feature, args=('risk_id_phnab',))
    # p3 = multiprocessing.Process(target=a.get_all_feature, args=('risk_id_phnac',))
    # p4 = multiprocessing.Process(target=a.get_all_feature, args=('risk_id_phnad',))
    p1.start()
    # p2.start()
    # p3.start()
    # p4.start()
    # a.get_all_feature()
    # phn='15253945581'
    # a.get_maxNocom_sourcetype(111)
    #
    # f1=a.get_bankAndcollection_yellowpage()
    #
    # begin=cur_time = datetime.datetime.now()
    #
    # gender,age=a.get_jk_basic_fea(phn)
    # a.get_comm_fea(phn)
    # a.get_diantance_yellowpage_fea(phn)
    # a.get_maxNocom_sourcetype(phn)
    # ui=747375
    # a.consumption(ui)
    # a.credit(ui)
    # end=cur_time = datetime.datetime.now()
    # print begin
    # print end
    # print 'time', (end-begin).seconds
