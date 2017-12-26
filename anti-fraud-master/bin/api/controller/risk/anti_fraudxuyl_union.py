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
        self.mhost1 = dconf.mg_host1
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
        self.db_sudaibear = create_engine(dconf.DB_USER_BASIC_INFO_SUDAIBEAR, encoding='utf-8', pool_recycle=30,
                                          poolclass=NullPool)
        self.Session = sessionmaker(bind=self.db_engine)
        self.Sessionsudai = sessionmaker(bind=self.db_sudaibear)
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

    # def init_mg_conn_old(self):
    #     self.conn_old = pm.MongoClient([dconf.o_mg_host1, dconf.o_mg_host2], replicaSet=dconf.o_mg_replicat_set,
    #                                    maxPoolSize=10)
    #     self.conn_old[dconf.o_mg_db].authenticate(dconf.o_mg_uname, dconf.o_mg_passwd)
    #     try:
    #         self.conn_old.server_info()
    #     except Exception, e:
    #         JKLOG.Error([('e', 'mongodb error'), ('exp type', str(e))])
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
    def get_union_fea(self,phn):
        union_dic={}
        sfile='featurename_online'
        feadic={}
        iddic={}
        with open(sfile,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                # print linelist
                feadic['bank.'+linelist[1]]=linelist[2]
                iddic['bank.'+linelist[1]]=linelist[0]
                if linelist[-1]=='num':
                    union_dic['bank.'+linelist[1]]=-1
                else:
                    union_dic['bank.'+linelist[1]]='-1'
        # print '111',len(union_dic)
        c = self.get_mg_connlbs(dconf.mg_collection_detail)
        cinfo = c.find_one({'mobile': phn})
        if not cinfo:
            JKLOG.Info([('find', 'mongodb'), ('phone', phn), ('info', 'no phn bank')])
            return union_dic
        if 'cardsStr' not in cinfo:
            return union_dic
        lately_time = datetime.datetime.now().strftime("%Y-%m-%d")
        lately_time_date = datetime.datetime.strptime(lately_time, "%Y-%m-%d")
        # print lately_time
        # anchor1 = self.before_some_days(lately_time, 180)
        try:
            # print type(cinfo['createDate'])
            lately_time_date = cinfo['createDate']
            # lately_time = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
            # anchor1 = self.before_some_days(lately_time, 180)
        except Exception, e:
            JKLOG.Error([('data', 'createDate failue'), ('error', str(e))])

        # print 'lately_time_date',type(lately_time_date)
        try:
            if 'cardsStr' in cinfo:
                ul = json.loads(cinfo['cardsStr'])
                # print ul
                for uj in ul:
                    uv = ul[uj]
                    if 'bankType' in uv and 'resultDetail' in uv:
                        if uv['bankType'] == '1':
                            resultdetail = uv['resultDetail']
                            rejson = json.loads(resultdetail)
                            # print rejson
                            if 'S0003' in rejson:
                                union_dic['bank.evection_num_12']=float(rejson['S0003'])
                                # if 'S0003'!=feadic['bank.evection_num_12']:
                                #     print 'S0003',feadic['bank.evection_num_12']
                                # print union_dic['bank.evection_num_12']
                                # evection_num_12 =float(rejson['S0544'])
                            if 'S0517' in rejson:
                                union_dic['bank.account_type'] = rejson['S0517']
                            if 'S0462' in rejson:
                                union_dic['bank.bank_card_type'] = rejson['S0462']
                            if 'S0464' in rejson:
                                union_dic['bank.bank_card_nature'] = rejson['S0464']
                            if 'S0465' in rejson:
                                union_dic['bank.bank_card_brand'] = rejson['S0465']
                            if 'S0466' in rejson:
                                union_dic['bank.bank_card_product'] = rejson['S0466']
                            if 'S0467' in rejson:
                                union_dic['bank.bank_card_grade'] = rejson['S0467']
                            if 'S0468' in rejson:
                                union_dic['bank.bank_card_name'] = rejson['S0468']
                            if 'S0469' in rejson:
                                union_dic['bank.bank_card_grant_name'] = rejson['S0469']
                            if 'S0505' in rejson:
                                union_dic['bank.2011_begin_transaction_month_num'] = float(rejson['S0505'])
                            if 'S0506' in rejson:
                                transaction_time=rejson['S0506']
                                if transaction_time!='NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.2011_first_transaction_time']=\
                                        int(round(1.0 * (lately_time_date - transaction_time_date).days / 30))
                            if 'S0507' in rejson:
                                transaction_time=rejson['S0507']
                                # print 'S0135', transaction_time
                                if transaction_time != 'NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.2011_recently_transaction_interval']=\
                                        (lately_time_date - transaction_time_date).days
                            if 'S0508' in rejson:
                                union_dic['bank.2011_recently_transaction_city'] = rejson['S0508']
                            if 'S0509' in rejson:
                                union_dic['bank.2011_recently_transaction_money'] = float(rejson['S0509'])
                            if 'S0511' in rejson:
                                union_dic['bank.2011_all_consume_transaction_money'] = float(rejson['S0511'])
                            if 'S0512' in rejson:
                                union_dic['bank.2011_all_consume_transaction_num'] = float(rejson['S0512'])
                            if 'S0513' in rejson:
                                union_dic['bank.2011_all_transaction_days'] = float(rejson['S0513'])
                            if 'S0514' in rejson:
                                if rejson['S0514']!='NA':
                                    max_type=rejson['S0514'].strip().split(';')
                                    union_dic['bank.2011_max_transaction_money'] = float(max_type[0])
                                    union_dic['bank.2011_max_transaction_money_type'] = max_type[1]
                            if 'S0515' in rejson:
                                ratio = rejson['S0515'].strip().split('%')
                                union_dic['bank.2011_transaction_lively_month_ratio'] = float(ratio[0])/100
                            if 'S0516' in rejson:
                                union_dic['bank.house_property_ratepaying_money'] = float(rejson['S0516'])
                            if 'S0660' in rejson:
                                if rejson['S0660']!=u'未知':
                                    provide = rejson['S0660'].strip().split(';')
                                    union_dic['bank.provide_bank_card_province'] = provide[0]
                                    if len(provide) < 2:
                                        union_dic['bank.provide_bank_card_city'] = '-1'
                                    else:
                                        union_dic['bank.provide_bank_card_city']=provide[1]
                            if 'S0661' in rejson:
                                union_dic['bank.high_stage_bank_card'] = rejson['S0661']
                            if 'S0510' in rejson:
                                union_dic['bank.2011_recently_transaction_commercial_tenant'] = rejson['S0510']
                            if 'S0502' in rejson:
                                union_dic['bank.territory_mobility'] = float(rejson['S0502'])
                            if 'S0018' in rejson:
                                union_dic['bank.transaction_channel_num_12'] = float(rejson['S0018'])
                            if 'S0134' in rejson:
                                union_dic['bank.recently_transaction_money_12'] = float(rejson['S0134'])
                            if 'S0135' in rejson:
                                transaction_time=rejson['S0135']
                                # print 'S0135', transaction_time
                                if transaction_time != 'NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.recently_transaction_time_12']=\
                                        int(round(1.0 * (lately_time_date - transaction_time_date).days / 30))
                            if 'S0476' in rejson:
                                union_dic['bank.consume_level_12'] = rejson['S0476']
                            if 'S0480' in rejson:
                                union_dic['bank.consume_gender_12'] = float(rejson['S0480'])
                            if 'S0481' in rejson:
                                union_dic['bank.consume_age_12'] = float(rejson['S0481'])
                            if 'S0615' in rejson:
                                union_dic['bank.transaction_type_num_12'] = float(rejson['S0615'])
                            if 'S0617' in rejson:
                                union_dic['bank.transaction_max_interval_days_12'] = float(rejson['S0617'])
                            if 'S0672' in rejson:
                                union_dic['bank.public_utility_pay_money_12'] = float(rejson['S0672'])
                            if 'S0673' in rejson:
                                union_dic['bank.public_utility_pay_num_12'] = float(rejson['S0673'])
                            if 'S0120' in rejson:
                                ratio = rejson['S0120'].strip().split('%')
                                union_dic['bank.consume_money_nationwide_rank'] = float(ratio[0])/100
                            if 'S0121' in rejson:
                                ratio = rejson['S0121'].strip().split('%')
                                union_dic['bank.consume_money_nationwide_rank_6'] = float(ratio[0])/100
                            if u'S0122' in rejson:
                                # print rejson[u'S0122']
                                ratio = rejson[u'S0122'].strip().split('%')
                                union_dic['bank.consume_money_nationwide_rank_12'] = float(ratio[0])/100
                            if 'S0123' in rejson:
                                ratio = rejson['S0123'].strip().split('%')
                                union_dic['bank.consume_num_nationwide_rank'] = float(ratio[0])/100
                            if 'S0124' in rejson:
                                ratio = rejson['S0124'].strip().split('%')
                                union_dic['bank.consume_num_nationwide_rank_6'] = float(ratio[0])/100
                            if 'S0125' in rejson:
                                ratio = rejson['S0125'].strip().split('%')
                                union_dic['bank.consume_num_nationwide_rank_12'] = float(ratio[0])/100
                            if 'S0474' in rejson:
                                union_dic['bank.repay_ability_monet_12'] = float(rejson['S0474'])
                            if 'S0542' in rejson:
                                union_dic['bank.transaction_average_money_12'] = float(rejson['S0542'])
                            if 'S0547' in rejson:
                                union_dic['bank.consume_low_month_num_12'] = float(rejson['S0547'])
                            if 'S0551' in rejson:
                                union_dic['bank.consume_money_12'] = float(rejson['S0551'])
                            if 'S0554' in rejson:
                                union_dic['bank.consume_num_12'] = float(rejson['S0554'])
                            if 'S0559' in rejson:
                                ratio = rejson['S0559'].strip().split(';')
                                ratio = ratio[1].strip().split('%')
                                union_dic['bank.transaction_lively_days_ratio'] = float(ratio[0])/100
                            if 'S0598' in rejson:
                                ratio = rejson['S0598'].strip().split('%')
                                union_dic['bank.transaction_low_money_ratio'] = float(ratio[0])/100
                            if 'S0599' in rejson:
                                ratio = rejson['S0599'].strip().split('%')
                                union_dic['bank.transaction_medium_money_ratio'] = float(ratio[0])/100
                            if 'S0600' in rejson:
                                ratio = rejson['S0600'].strip().split('%')
                                union_dic['bank.transaction_high_money_ratio'] = float(ratio[0])/100
                            if 'S0604' in rejson:
                                max_type=rejson['S0604'].strip().split(';')
                                union_dic['bank.max_transaction_money_12'] = float(max_type[0])
                                union_dic['bank.max_transaction_money_type_12'] = max_type[1]
                            if 'S0648' in rejson:
                                union_dic['bank.transaction_MCC_num_12'] = float(rejson['S0648'])
                            if 'S0570' in rejson:
                                union_dic['bank.not_wholesale_transaction_average_month_money_12'] = float(rejson['S0570'])
                            if 'S0573' in rejson:
                                union_dic['bank.transaction_territory_preference_12'] = rejson['S0573']
                            if 'S0127' in rejson:
                                ratio = rejson['S0127'].strip().split('%')
                                union_dic['bank.consume_money_city_rank_6'] = float(ratio[0])/100
                            if 'S0128' in rejson:
                                ratio = rejson['S0128'].strip().split('%')
                                union_dic['bank.portion_consume_money_city_rank_12'] = float(ratio[0])/100
                            if 'S0130' in rejson:
                                ratio = rejson['S0130'].strip().split('%')
                                union_dic['bank.consume_num_city_rank_6'] = float(ratio[0])/100
                            if 'S0131' in rejson:
                                ratio = rejson['S0131'].strip().split('%')
                                union_dic['bank.portion_consume_num_city_rank_12'] = float(ratio[0])/100
                            if 'S0483' in rejson:
                                union_dic['bank.transaction_max_num_city_12'] = rejson['S0483']
                            if 'S0504' in rejson:
                                union_dic['bank.transaction_max_days_city_12'] = rejson['S0504']
                            if 'S0563' in rejson:
                                ratio = rejson['S0563'].strip().split('%')
                                union_dic['bank.consume_money_city_rank_12'] = float(ratio[0])/100
                            if 'S0564' in rejson:
                                ratio = rejson['S0564'].strip().split('%')
                                union_dic['bank.consume_num_city_rank_12'] = float(ratio[0])/100
                            if 'S0565' in rejson:
                                ratio = rejson['S0565'].strip().split('%')
                                union_dic['bank.transaction_days_city_rank_12'] = float(ratio[0])/100
                            if 'S0566' in rejson:
                                ratio = rejson['S0566'].strip().split('%')
                                union_dic['bank.transaction_average_money_city_rank_12'] = float(ratio[0])/100
                            if 'S0567' in rejson:
                                ratio = rejson['S0567'].strip().split('%')
                                union_dic['bank.consume_average_money_city_rank_12'] = float(ratio[0])/100
                            if 'S0603' in rejson:
                                max_type=rejson['S0603'].strip().split(';')
                                union_dic['bank.max_transaction_money_6'] = float(max_type[0])
                                union_dic['bank.max_transaction_money_type_6'] = max_type[1]
                            if 'S0656' in rejson:
                                ratio = rejson['S0656'].strip().split('%')
                                union_dic['bank.working_consume_num_ratio'] = float(ratio[0]) / 100
                            if 'S0075' in rejson:
                                union_dic['bank.portion_consume_money_12'] = float(rejson['S0075'])
                            if 'S0078' in rejson:
                                union_dic['bank.portion_consume_num_12'] = float(rejson['S0078'])
                            if 'S0108' in rejson:
                                union_dic['bank.transaction_max_interval_days'] = float(rejson['S0108'])
                            if 'S0109' in rejson:
                                union_dic['bank.transaction_max_interval_days_6'] = float(rejson['S0109'])
                            if 'S0110' in rejson:
                                union_dic['bank.transaction_filtrate_max_interval_days_12'] = float(rejson['S0110'])
                            if 'S0111' in rejson:
                                union_dic['bank.consume_max_interval_days'] = float(rejson['S0111'])
                            if 'S0112' in rejson:
                                union_dic['bank.consume_max_interval_days_6'] = float(rejson['S0112'])
                            if 'S0113' in rejson:
                                union_dic['bank.consume_max_interval_days_12'] = float(rejson['S0113'])
                            if 'S0114' in rejson:
                                union_dic['bank.max_consume_money'] = float(rejson['S0114'])
                            if 'S0115' in rejson:
                                union_dic['bank.max_consume_money_6'] = float(rejson['S0115'])
                            if 'S0116' in rejson:
                                union_dic['bank.max_consume_money_12'] = float(rejson['S0116'])
                            if 'S0117' in rejson:
                                union_dic['bank.max_consume_num'] = float(rejson['S0117'])
                            if 'S0118' in rejson:
                                union_dic['bank.max_consume_num_6'] = float(rejson['S0118'])
                            if 'S0119' in rejson:
                                union_dic['bank.max_consume_num_12'] = float(rejson['S0119'])
                            if 'S0602' in rejson:
                                max_type = rejson['S0602'].strip().split(';')
                                union_dic['bank.max_transaction_money_3'] = float(max_type[0])
                                union_dic['bank.max_transaction_money_type_3'] = max_type[1]
                            if 'S0276' in rejson:
                                union_dic['bank.max_defeated_transaction_num'] = float(rejson['S0276'])
                            if 'S0277' in rejson:
                                union_dic['bank.max_defeated_transaction_num_6'] = float(rejson['S0277'])
                            if 'S0278' in rejson:
                                union_dic['bank.max_defeated_transaction_num_12'] = float(rejson['S0278'])
                            if 'S0279' in rejson:
                                union_dic['bank.max_accumulation_enchashment_money'] = float(rejson['S0279'])
                            if 'S0280' in rejson:
                                union_dic['bank.max_accumulation_enchashment_money_6'] = float(rejson['S0280'])
                            if 'S0281' in rejson:
                                union_dic['bank.max_accumulation_enchashment_money_12'] = float(rejson['S0281'])
                            if 'S0282' in rejson:
                                union_dic['bank.max_accumulation_enchashment_num'] = float(rejson['S0282'])
                            if 'S0283' in rejson:
                                union_dic['bank.max_accumulation_enchashment_num_6'] = float(rejson['S0283'])
                            if 'S0284' in rejson:
                                union_dic['bank.max_accumulation_enchashment_num_12'] = float(rejson['S0284'])
                            if 'S0174' in rejson:
                                ratio = rejson['S0174'].strip().split('%')
                                union_dic['bank.night_transaction_money_ratio_12'] = float(ratio[0]) / 100
                            if 'S0175' in rejson:
                                ratio = rejson['S0175'].strip().split('%')
                                union_dic['bank.night_transaction_num_ratio_12'] = float(ratio[0]) / 100
                            if 'S0180' in rejson:
                                union_dic['bank.recreation_consume_money'] = float(rejson['S0180'])
                            if 'S0181' in rejson:
                                union_dic['bank.recreation_consume_money_6'] = float(rejson['S0181'])
                            if 'S0182' in rejson:
                                union_dic['bank.recreation_consume_money_12'] = float(rejson['S0182'])
                            if 'S0183' in rejson:
                                union_dic['bank.recreation_consume_num'] = float(rejson['S0183'])
                            if 'S0184' in rejson:
                                union_dic['bank.recreation_consume_num_6'] = float(rejson['S0184'])
                            if 'S0185' in rejson:
                                union_dic['bank.recreation_consume_num_12'] = float(rejson['S0185'])
                            if 'S0186' in rejson:
                                union_dic['bank.catering_consume_money'] = float(rejson['S0186'])
                            if 'S0187' in rejson:
                                union_dic['bank.catering_consume_money_6'] = float(rejson['S0187'])
                            if 'S0188' in rejson:
                                union_dic['bank.catering_consume_money_12'] = float(rejson['S0188'])
                            if 'S0189' in rejson:
                                union_dic['bank.catering_consume_num'] = float(rejson['S0189'])
                            if 'S0190' in rejson:
                                union_dic['bank.catering_consume_num_6'] = float(rejson['S0190'])
                            if 'S0191' in rejson:
                                union_dic['bank.catering_consume_num_12'] = float(rejson['S0191'])
                            if 'S0192' in rejson:
                                union_dic['bank.travel_consume_money'] = float(rejson['S0192'])
                            if 'S0193' in rejson:
                                union_dic['bank.travel_consume_money_6'] = float(rejson['S0193'])
                            if 'S0194' in rejson:
                                union_dic['bank.travel_consume_money_12'] = float(rejson['S0194'])
                            if 'S0195' in rejson:
                                union_dic['bank.travel_consume_num'] = float(rejson['S0195'])
                            if 'S0196' in rejson:
                                union_dic['bank.travel_consume_num_6'] = float(rejson['S0196'])
                            if 'S0197' in rejson:
                                union_dic['bank.travel_consume_num_12'] = float(rejson['S0197'])
                            if 'S0198' in rejson:
                                union_dic['bank.medicine_consume_money'] = float(rejson['S0198'])
                            if 'S0199' in rejson:
                                union_dic['bank.medicine_consume_money_6'] = float(rejson['S0199'])
                            if 'S0200' in rejson:
                                union_dic['bank.medicine_consume_money_12'] = float(rejson['S0200'])
                            if 'S0201' in rejson:
                                union_dic['bank.medicine_consume_num'] = float(rejson['S0201'])
                            if 'S0202' in rejson:
                                union_dic['bank.medicine_consume_num_6'] = float(rejson['S0202'])
                            if 'S0203' in rejson:
                                union_dic['bank.medicine_consume_num_12'] = float(rejson['S0203'])
                            if 'S0204' in rejson:
                                union_dic['bank.public_utility_pay_money'] = float(rejson['S0204'])
                            if 'S0205' in rejson:
                                union_dic['bank.public_utility_pay_money_6'] = float(rejson['S0205'])
                            if 'S0207' in rejson:
                                union_dic['bank.public_utility_pay_num'] = float(rejson['S0207'])
                            if 'S0208' in rejson:
                                union_dic['bank.public_utility_pay_num_6'] = float(rejson['S0208'])
                            if 'S0210' in rejson:
                                union_dic['bank.credit_repayment_money'] = float(rejson['S0210'])
                            if 'S0211' in rejson:
                                union_dic['bank.credit_repayment_money_6'] = float(rejson['S0211'])
                            if 'S0213' in rejson:
                                union_dic['bank.credit_repayment_num'] = float(rejson['S0213'])
                            if 'S0214' in rejson:
                                union_dic['bank.credit_repayment_num_6'] = float(rejson['S0214'])
                            if 'S0216' in rejson:
                                union_dic['bank.petrol_station_consume_money'] = float(rejson['S0216'])
                            if 'S0217' in rejson:
                                union_dic['bank.petrol_station_consume_money_6'] = float(rejson['S0217'])
                            if 'S0218' in rejson:
                                union_dic['bank.petrol_station_consume_money_12'] = float(rejson['S0218'])
                            if 'S0219' in rejson:
                                union_dic['bank.petrol_station_consume_num'] = float(rejson['S0219'])
                            if 'S0220' in rejson:
                                union_dic['bank.petrol_station_consume_num_6'] = float(rejson['S0220'])
                            if 'S0221' in rejson:
                                union_dic['bank.petrol_station_consume_num_12'] = float(rejson['S0221'])
                            if 'S0222' in rejson:
                                union_dic['bank.cosmetics_consume_money'] = float(rejson['S0222'])
                            if 'S0223' in rejson:
                                union_dic['bank.cosmetics_consume_money_6'] = float(rejson['S0223'])
                            if 'S0224' in rejson:
                                union_dic['bank.cosmetics_consume_money_12'] = float(rejson['S0224'])
                            if 'S0225' in rejson:
                                union_dic['bank.cosmetics_consume_num'] = float(rejson['S0225'])
                            if 'S0226' in rejson:
                                union_dic['bank.cosmetics_consume_num_6'] = float(rejson['S0226'])
                            if 'S0227' in rejson:
                                union_dic['bank.cosmetics_consume_num_12'] = float(rejson['S0227'])
                            if 'S0228' in rejson:
                                union_dic['bank.accommodation_consume_money'] = float(rejson['S0228'])
                            if 'S0229' in rejson:
                                union_dic['bank.accommodation_consume_money_6'] = float(rejson['S0229'])
                            if 'S0230' in rejson:
                                union_dic['bank.accommodation_consume_money_12'] = float(rejson['S0230'])
                            if 'S0231' in rejson:
                                union_dic['bank.accommodation_consume_num'] = float(rejson['S0231'])
                            if 'S0232' in rejson:
                                union_dic['bank.accommodation_consume_num_6'] = float(rejson['S0232'])
                            if 'S0233' in rejson:
                                union_dic['bank.accommodation_consume_num_12'] = float(rejson['S0233'])
                            if 'S0234' in rejson:
                                union_dic['bank.massage_consume_money'] = float(rejson['S0234'])
                            if 'S0235' in rejson:
                                union_dic['bank.massage_consume_money_6'] = float(rejson['S0235'])
                            if 'S0236' in rejson:
                                union_dic['bank.massage_consume_money_12'] = float(rejson['S0236'])
                            if 'S0237' in rejson:
                                union_dic['bank.massage_consume_num'] = float(rejson['S0237'])
                            if 'S0238' in rejson:
                                union_dic['bank.massage_consume_num_6'] = float(rejson['S0238'])
                            if 'S0239' in rejson:
                                union_dic['bank.massage_consume_num_12'] = float(rejson['S0239'])
                            if 'S0240' in rejson:
                                union_dic['bank.top_grade_exercise_consume_money'] = float(rejson['S0240'])
                            if 'S0241' in rejson:
                                union_dic['bank.top_grade_exercise_consume_money_6'] = float(rejson['S0241'])
                            if 'S0242' in rejson:
                                union_dic['bank.top_grade_exercise_consume_money_12'] = float(rejson['S0242'])
                            if 'S0243' in rejson:
                                union_dic['bank.top_grade_exercise_consume_num'] = float(rejson['S0243'])
                            if 'S0244' in rejson:
                                union_dic['bank.top_grade_exercise_consume_num_6'] = float(rejson['S0244'])
                            if 'S0245' in rejson:
                                union_dic['bank.top_grade_exercise_consume_num_12'] = float(rejson['S0245'])
                            if 'S0246' in rejson:
                                union_dic['bank.car_consume_money'] = float(rejson['S0246'])
                            if 'S0247' in rejson:
                                union_dic['bank.car_consume_money_6'] = float(rejson['S0247'])
                            if 'S0248' in rejson:
                                union_dic['bank.car_consume_money_12'] = float(rejson['S0248'])
                            if 'S0249' in rejson:
                                union_dic['bank.car_consume_num'] = float(rejson['S0249'])
                            if 'S0250' in rejson:
                                union_dic['bank.car_consume_num_6'] = float(rejson['S0250'])
                            if 'S0251' in rejson:
                                union_dic['bank.car_consume_num_12'] = float(rejson['S0251'])
                            if 'S0252' in rejson:
                                union_dic['bank.drink_site_consume_money'] = float(rejson['S0252'])
                            if 'S0253' in rejson:
                                union_dic['bank.drink_site_consume_money_6'] = float(rejson['S0253'])
                            if 'S0254' in rejson:
                                union_dic['bank.drink_site_consume_money_12'] = float(rejson['S0254'])
                            if 'S0255' in rejson:
                                union_dic['bank.drink_site_consume_num'] = float(rejson['S0255'])
                            if 'S0256' in rejson:
                                union_dic['bank.drink_site_consume_num_6'] = float(rejson['S0256'])
                            if 'S0257' in rejson:
                                union_dic['bank.drink_site_consume_num_12'] = float(rejson['S0257'])
                            if 'S0258' in rejson:
                                union_dic['bank.KTV_consume_money'] = float(rejson['S0258'])
                            if 'S0259' in rejson:
                                union_dic['bank.KTV_consume_money_6'] = float(rejson['S0259'])
                            if 'S0260' in rejson:
                                union_dic['bank.KTV_consume_money_12'] = float(rejson['S0260'])
                            if 'S0261' in rejson:
                                union_dic['bank.KTV_consume_num'] = float(rejson['S0261'])
                            if 'S0262' in rejson:
                                union_dic['bank.KTV_consume_num_6'] = float(rejson['S0262'])
                            if 'S0263' in rejson:
                                union_dic['bank.KTV_consume_num_12'] = float(rejson['S0263'])
                            if 'S0264' in rejson:
                                union_dic['bank.superstore_consume_money'] = float(rejson['S0264'])
                            if 'S0265' in rejson:
                                union_dic['bank.superstore_consume_money_6'] = float(rejson['S0265'])
                            if 'S0266' in rejson:
                                union_dic['bank.superstore_consume_money_12'] = float(rejson['S0266'])
                            if 'S0267' in rejson:
                                union_dic['bank.superstore_consume_num'] = float(rejson['S0267'])
                            if 'S0268' in rejson:
                                union_dic['bank.superstore_consume_num_6'] = float(rejson['S0268'])
                            if 'S0269' in rejson:
                                union_dic['bank.superstore_consume_num_12'] = float(rejson['S0269'])
                            if 'S0378' in rejson:
                                union_dic['bank.night_transaction_money'] = float(rejson['S0378'])
                            if 'S0379' in rejson:
                                union_dic['bank.night_transaction_money_6'] = float(rejson['S0379'])
                            if 'S0381' in rejson:
                                union_dic['bank.night_transaction_num'] = float(rejson['S0381'])
                            if 'S0382' in rejson:
                                union_dic['bank.night_transaction_num_6'] = float(rejson['S0382'])
                            if 'S0390' in rejson:
                                union_dic['bank.aviation_consume_money'] = float(rejson['S0390'])
                            if 'S0391' in rejson:
                                union_dic['bank.aviation_consume_money_6'] = float(rejson['S0391'])
                            if 'S0392' in rejson:
                                union_dic['bank.aviation_consume_money_12'] = float(rejson['S0392'])
                            if 'S0393' in rejson:
                                union_dic['bank.aviation_consume_num'] = float(rejson['S0393'])
                            if 'S0394' in rejson:
                                union_dic['bank.aviation_consume_num_6'] = float(rejson['S0394'])
                            if 'S0395' in rejson:
                                union_dic['bank.aviation_consume_num_12'] = float(rejson['S0395'])
                            if 'S0396' in rejson:
                                union_dic['bank.railway_consume_money'] = float(rejson['S0396'])
                            if 'S0397' in rejson:
                                union_dic['bank.railway_consume_money_6'] = float(rejson['S0397'])
                            if 'S0398' in rejson:
                                union_dic['bank.railway_consume_money_12'] = float(rejson['S0398'])
                            if 'S0399' in rejson:
                                union_dic['bank.railway_consume_num'] = float(rejson['S0399'])
                            if 'S0400' in rejson:
                                union_dic['bank.railway_consume_num_6'] = float(rejson['S0400'])
                            if 'S0401' in rejson:
                                union_dic['bank.railway_consume_num_12'] = float(rejson['S0401'])
                            if 'S0402' in rejson:
                                union_dic['bank.auction_consume_money'] = float(rejson['S0402'])
                            if 'S0403' in rejson:
                                union_dic['bank.auction_consume_money_6'] = float(rejson['S0403'])
                            if 'S0404' in rejson:
                                union_dic['bank.auction_consume_money_12'] = float(rejson['S0404'])
                            if 'S0405' in rejson:
                                union_dic['bank.auction_consume_num'] = float(rejson['S0405'])
                            if 'S0406' in rejson:
                                union_dic['bank.auction_consume_num_6'] = float(rejson['S0406'])
                            if 'S0407' in rejson:
                                union_dic['bank.auction_consume_num_12'] = float(rejson['S0407'])
                            if 'S0408' in rejson:
                                union_dic['bank.security_consume_money'] = float(rejson['S0408'])
                            if 'S0409' in rejson:
                                union_dic['bank.security_consume_money_6'] = float(rejson['S0409'])
                            if 'S0410' in rejson:
                                union_dic['bank.security_consume_money_12'] = float(rejson['S0410'])
                            if 'S0411' in rejson:
                                union_dic['bank.security_consume_num'] = float(rejson['S0411'])
                            if 'S0412' in rejson:
                                union_dic['bank.security_consume_num_6'] = float(rejson['S0412'])
                            if 'S0413' in rejson:
                                union_dic['bank.security_consume_num_12'] = float(rejson['S0413'])
                            if 'S0414' in rejson:
                                union_dic['bank.fine_jewelry_consume_money'] = float(rejson['S0414'])
                            if 'S0415' in rejson:
                                union_dic['bank.fine_jewelry_consume_money_6'] = float(rejson['S0415'])
                            if 'S0416' in rejson:
                                union_dic['bank.fine_jewelry_consume_money_12'] = float(rejson['S0416'])
                            if 'S0417' in rejson:
                                union_dic['bank.fine_jewelry_consume_num'] = float(rejson['S0417'])
                            if 'S0418' in rejson:
                                union_dic['bank.fine_jewelry_consume_num_6'] = float(rejson['S0418'])
                            if 'S0419' in rejson:
                                union_dic['bank.fine_jewelry_consume_num_12'] = float(rejson['S0419'])
                            if 'S0420' in rejson:
                                union_dic['bank.antique_consume_money'] = float(rejson['S0420'])
                            if 'S0421' in rejson:
                                union_dic['bank.antique_consume_money_6'] = float(rejson['S0421'])
                            if 'S0422' in rejson:
                                union_dic['bank.antique_consume_money_12'] = float(rejson['S0422'])
                            if 'S0423' in rejson:
                                union_dic['bank.antique_consume_num'] = float(rejson['S0423'])
                            if 'S0424' in rejson:
                                union_dic['bank.antique_consume_num_6'] = float(rejson['S0424'])
                            if 'S0425' in rejson:
                                union_dic['bank.antique_consume_num_12'] = float(rejson['S0425'])
                            if 'S0426' in rejson:
                                union_dic['bank.lottery_consume_money'] = float(rejson['S0426'])
                            if 'S0427' in rejson:
                                union_dic['bank.lottery_consume_money_6'] = float(rejson['S0427'])
                            if 'S0429' in rejson:
                                union_dic['bank.lottery_consume_num'] = float(rejson['S0429'])
                            if 'S0430' in rejson:
                                union_dic['bank.lottery_consume_num_6'] = float(rejson['S0430'])
                            if 'S0431' in rejson:
                                union_dic['bank.lottery_consume_num_12'] = float(rejson['S0431'])
                            if 'S0432' in rejson:
                                union_dic['bank.law_service_consume_money'] = float(rejson['S0432'])
                            if 'S0433' in rejson:
                                union_dic['bank.law_service_consume_money_6'] = float(rejson['S0433'])
                            if 'S0434' in rejson:
                                union_dic['bank.law_service_consume_money_12'] = float(rejson['S0434'])
                            if 'S0435' in rejson:
                                union_dic['bank.law_service_consume_num'] = float(rejson['S0435'])
                            if 'S0436' in rejson:
                                union_dic['bank.law_service_consume_num_6'] = float(rejson['S0436'])
                            if 'S0437' in rejson:
                                union_dic['bank.law_service_consume_num_12'] = float(rejson['S0437'])
                            if 'S0438' in rejson:
                                union_dic['bank.penalty_money'] = float(rejson['S0438'])
                            if 'S0439' in rejson:
                                union_dic['bank.penalty_money_6'] = float(rejson['S0439'])
                            if 'S0440' in rejson:
                                union_dic['bank.penalty_money_12'] = float(rejson['S0440'])
                            if 'S0441' in rejson:
                                union_dic['bank.penalty_num'] = float(rejson['S0441'])
                            if 'S0442' in rejson:
                                union_dic['bank.penalty_num_6'] = float(rejson['S0442'])
                            if 'S0443' in rejson:
                                union_dic['bank.penalty_num_12'] = float(rejson['S0443'])
                            if 'S0444' in rejson:
                                union_dic['bank.ratepaying_money'] = float(rejson['S0444'])
                            if 'S0445' in rejson:
                                union_dic['bank.ratepaying_money_6'] = float(rejson['S0445'])
                            if 'S0446' in rejson:
                                union_dic['bank.ratepaying_money_12'] = float(rejson['S0446'])
                            if 'S0447' in rejson:
                                union_dic['bank.ratepaying_num'] = float(rejson['S0447'])
                            if 'S0448' in rejson:
                                union_dic['bank.ratepaying_num_6'] = float(rejson['S0448'])
                            if 'S0449' in rejson:
                                union_dic['bank.ratepaying_num_12'] = float(rejson['S0449'])
                            if 'S0450' in rejson:
                                union_dic['bank.insurance_money'] = float(rejson['S0450'])
                            if 'S0451' in rejson:
                                union_dic['bank.insurance_money_6'] = float(rejson['S0451'])
                            if 'S0452' in rejson:
                                union_dic['bank.insurance_money_12'] = float(rejson['S0452'])
                            if 'S0453' in rejson:
                                union_dic['bank.insurance_num'] = float(rejson['S0453'])
                            if 'S0454' in rejson:
                                union_dic['bank.insurance_num_6'] = float(rejson['S0454'])
                            if 'S0455' in rejson:
                                union_dic['bank.insurance_num_12'] = float(rejson['S0455'])
                            if 'S0456' in rejson:
                                union_dic['bank.donation_money'] = float(rejson['S0456'])
                            if 'S0457' in rejson:
                                union_dic['bank.donation_money_6'] = float(rejson['S0457'])
                            if 'S0458' in rejson:
                                union_dic['bank.donation_money_12'] = float(rejson['S0458'])
                            if 'S0459' in rejson:
                                union_dic['bank.donation_num'] = float(rejson['S0459'])
                            if 'S0460' in rejson:
                                union_dic['bank.donation_num_6'] = float(rejson['S0460'])
                            if 'S0461' in rejson:
                                union_dic['bank.donation_num_12'] = float(rejson['S0461'])
                            if 'S0678' in rejson:
                                union_dic['bank.department_store_consume_money_12'] = float(rejson['S0678'])
                            if 'S0679' in rejson:
                                union_dic['bank.daily_supplies_consume_money_12'] = float(rejson['S0679'])
                            if 'S0558' in rejson:
                                ratio = rejson['S0558'].strip().split('%')
                                union_dic['bank.recent_transaction_money_increase_rate'] = float(ratio[0]) / 100
                            if 'S0601' in rejson:
                                max_type = rejson['S0601'].strip().split(';')
                                union_dic['bank.max_transaction_money'] = float(max_type[0])
                                union_dic['bank.max_transaction_money_type'] = max_type[1]
                            if 'S0626' in rejson:
                                union_dic['bank.recently_defeated_transaction_reason_12'] = rejson['S0626']
                            if 'S0627' in rejson:
                                transaction_time=rejson['S0627']
                                # print 'S0627', transaction_time
                                if transaction_time != 'NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.recently_defeated_transaction_interval_12']=\
                                        (lately_time_date - transaction_time_date).days
                            if 'S0291' in rejson:
                                union_dic['bank.single_day_same_business_transaction_num'] = float(rejson['S0291'])
                            if 'S0292' in rejson:
                                union_dic['bank.single_day_same_business_transaction_num_6'] = float(rejson['S0292'])
                            if 'S0293' in rejson:
                                union_dic['bank.single_day_same_business_transaction_num_12'] = float(rejson['S0293'])
                            if 'S0297' in rejson:
                                union_dic['bank.single_day_same_business_transaction_min_interval'] = float(rejson['S0297'])
                            if 'S0298' in rejson:
                                union_dic['bank.single_day_same_business_transaction_min_interval_6'] = float(rejson['S0298'])
                            if 'S0299' in rejson:
                                union_dic['bank.single_day_same_business_transaction_min_interval_12'] = float(rejson['S0299'])
                            if 'S0300' in rejson:
                                union_dic['bank.span_city_consume_min_interval'] = float(rejson['S0300'])
                            if 'S0301' in rejson:
                                union_dic['bank.span_city_consume_min_interval_6'] = float(rejson['S0301'])
                            if 'S0302' in rejson:
                                union_dic['bank.span_city_consume_min_interval_12'] = float(rejson['S0302'])
                            if 'S0659' in rejson:
                                union_dic['bank.city_consume_expert_12'] = rejson['S0659']
                            if 'S0618' in rejson:
                                union_dic['bank.steal_card_risk_12'] = rejson['S0618']
                            if 'S0622' in rejson:
                                union_dic['bank.ingurgitation_card_12'] = rejson['S0622']
                            if 'S0623' in rejson:
                                union_dic['bank.exceed_num_astrict_transaction_12'] = rejson['S0623']
                            if 'S0624' in rejson:
                                union_dic['bank.exceed_money_astrict_transaction_12'] = rejson['S0624']
                            if 'S0625' in rejson:
                                union_dic['bank.insufficient_money_astrict_transaction_12'] = rejson['S0625']
                            if 'S0492' in rejson:
                                union_dic['bank.student_feature_12'] = rejson['S0492']
                            if 'S0372' in rejson:
                                union_dic['bank.tidiness_consume_money'] = float(rejson['S0372'])
                            if 'S0373' in rejson:
                                union_dic['bank.tidiness_consume_money_6'] = float(rejson['S0373'])
                            if 'S0374' in rejson:
                                union_dic['bank.tidiness_consume_money_12'] = float(rejson['S0374'])
                            if 'S0475' in rejson:
                                union_dic['bank.unemployed_12'] = rejson['S0475']
                            if 'S0303' in rejson:
                                union_dic['bank.one_hour_span_city_consume_num'] = float(rejson['S0303'])
                            if 'S0304' in rejson:
                                union_dic['bank.one_hour_span_city_consume_num_6'] = float(rejson['S0304'])
                            if 'S0305' in rejson:
                                union_dic['bank.one_hour_span_city_consume_num_12'] = float(rejson['S0305'])
                            if 'S0307' in rejson:
                                union_dic['bank.big_recorded_money_6'] = float(rejson['S0307'])
                            if 'S0308' in rejson:
                                union_dic['bank.big_recorded_money_12'] = float(rejson['S0308'])
                            if 'S0313' in rejson:
                                ratio = rejson['S0313'].strip().split('%')
                                union_dic['bank.big_recorded_transfer_money_ratio_6'] = float(ratio[0]) / 100
                            if 'S0314' in rejson:
                                ratio = rejson['S0314'].strip().split('%')
                                union_dic['bank.big_recorded_transfer_money_ratio_12'] = float(ratio[0]) / 100
                            if 'S0316' in rejson:
                                ratio = rejson['S0316'].strip().split('%')
                                union_dic['bank.big_recorded_enchashment_money_ratio_6'] = float(ratio[0]) / 100
                            if 'S0317' in rejson:
                                ratio = rejson['S0317'].strip().split('%')
                                union_dic['bank.big_recorded_enchashment_money_ratio_12'] = float(ratio[0]) / 100
                            if 'S0319' in rejson:
                                ratio = rejson['S0319'].strip().split('%')
                                union_dic['bank.big_recorded_consume_money_ratio_6'] = float(ratio[0]) / 100
                            if 'S0320' in rejson:
                                ratio = rejson['S0320'].strip().split('%')
                                union_dic['bank.big_recorded_consume_money_ratio_12'] = float(ratio[0]) / 100
                            if 'S0651' in rejson:
                                union_dic['bank.hospital_consume_num'] = float(rejson['S0651'])
                            if 'S0294' in rejson:
                                union_dic['bank.one_day_one_commercial_equality_corpus_consume_num'] = float(rejson['S0294'])
                            if 'S0295' in rejson:
                                union_dic['bank.one_day_one_commercial_equality_corpus_consume_num_6'] = float(rejson['S0295'])
                            if 'S0296' in rejson:
                                union_dic['bank.one_day_one_commercial_equality_corpus_consume_num_12'] = float(rejson['S0296'])
                            if 'S0470' in rejson:
                                union_dic['bank.2011_have_house'] = rejson['S0470']
                            if 'S0471' in rejson:
                                transaction_time=rejson['S0471']
                                # print 'S0471', transaction_time
                                if transaction_time != 'NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.2011_first_house_property_consume_interval']=\
                                        int(round(1.0 * (lately_time_date - transaction_time_date).days / 30))
                            if 'S0472' in rejson:
                                union_dic['bank.2011_have_car'] = rejson['S0472']
                            if 'S0473' in rejson:
                                transaction_time=rejson['S0473']
                                # print 'S0473', transaction_time
                                if transaction_time != 'NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.2011_first_car_consume_interval']=\
                                        int(round(1.0 * (lately_time_date - transaction_time_date).days / 30))
                            if 'S0663' in rejson:
                                union_dic['bank.2011_house_property_appraisement'] = float(rejson['S0663'])
                            if 'S0664' in rejson:
                                union_dic['bank.2011_car_appraisement'] = float(rejson['S0664'])
                            if 'S0684' in rejson:
                                union_dic['bank.bank_card_Recorded_money_6'] = float(rejson['S0684'])
                            if 'S0685' in rejson:
                                union_dic['bank.bank_card_Recorded_num_6'] = float(rejson['S0685'])
                            if 'S0686' in rejson:
                                union_dic['bank.bank_card_Recorded_money_12'] = float(rejson['S0686'])
                            if 'S0687' in rejson:
                                union_dic['bank.bank_card_Recorded_num_12'] = float(rejson['S0687'])
                            if 'S0055' in rejson:
                                union_dic['bank.online_shopping_money'] = float(rejson['S0055'])
                            if 'S0057' in rejson:
                                union_dic['bank.online_shopping_money_12'] = float(rejson['S0057'])
                            if 'S0058' in rejson:
                                union_dic['bank.online_shopping_num'] = float(rejson['S0058'])
                            if 'S0060' in rejson:
                                union_dic['bank.online_shopping_num_12'] = float(rejson['S0060'])
                            if 'S0652' in rejson:
                                union_dic['bank.2011_car_consume_money'] = float(rejson['S0652'])
                            if 'S0654' in rejson:
                                union_dic['bank.2011_car_consume_num'] = float(rejson['S0654'])
                            if 'S0049' in rejson:
                                union_dic['bank.credit_card_enchashment_money_12'] = float(rejson['S0049'])
                            if 'S0050' in rejson:
                                union_dic['bank.credit_card_enchashment_num_12'] = float(rejson['S0050'])
                            if 'S0094' in rejson:
                                union_dic['bank.foreign_currency_transaction_kind_num'] = float(rejson['S0094'])
                            if 'S0095' in rejson:
                                union_dic['bank.foreign_currency_transaction_kind_num_6'] = float(rejson['S0095'])
                            if 'S0096' in rejson:
                                union_dic['bank.foreign_currency_transaction_kind_num_12'] = float(rejson['S0096'])
                            if 'S0099' in rejson:
                                union_dic['bank.overseas_transaction_money_12'] = float(rejson['S0099'])
                            if 'S0106' in rejson:
                                ratio = rejson['S0106'].strip().split('%')
                                union_dic['bank.overseas_transaction_money_ratio_12'] = float(ratio[0]) / 100
                            if 'S0107' in rejson:
                                ratio = rejson['S0107'].strip().split('%')
                                union_dic['bank.overseas_transaction_num_ratio_12'] = float(ratio[0]) / 100
                            if 'S0681' in rejson:
                                union_dic['bank.overseas_consume_12'] = rejson['S0681']
                            if 'S0644' in rejson:
                                union_dic['bank.hospital_one_wholesale_point_money_12'] = float(rejson['S0644'])
                            if 'S0342' in rejson:
                                union_dic['bank.medium_transfer_accounts_money'] = float(rejson['S0342'])
                            if 'S0343' in rejson:
                                union_dic['bank.medium_transfer_accounts_money_6'] = float(rejson['S0343'])
                            if 'S0344' in rejson:
                                union_dic['bank.medium_transfer_accounts_money_12'] = float(rejson['S0344'])
                            if 'S0345' in rejson:
                                union_dic['bank.medium_transfer_accounts_num'] = float(rejson['S0345'])
                            if 'S0346' in rejson:
                                union_dic['bank.medium_transfer_accounts_num_6'] = float(rejson['S0346'])
                            if 'S0347' in rejson:
                                union_dic['bank.medium_transfer_accounts_num_12'] = float(rejson['S0347'])
                            if 'S0336' in rejson:
                                union_dic['bank.big_transfer_accounts_money'] = float(rejson['S0336'])
                            if 'S0337' in rejson:
                                union_dic['bank.big_transfer_accounts_money_6'] = float(rejson['S0337'])
                            if 'S0338' in rejson:
                                union_dic['bank.big_transfer_accounts_money_12'] = float(rejson['S0338'])
                            if 'S0339' in rejson:
                                union_dic['bank.big_transfer_accounts_num'] = float(rejson['S0339'])
                            if 'S0340' in rejson:
                                union_dic['bank.big_transfer_accounts_num_6'] = float(rejson['S0340'])
                            if 'S0341' in rejson:
                                union_dic['bank.big_transfer_accounts_num_12'] = float(rejson['S0341'])
                            if 'S0657' in rejson:
                                union_dic['bank.2011_get_married'] = rejson['S0657']
                            if 'S0493' in rejson:
                                union_dic['bank.2011_children_feature'] = rejson['S0493']
                            if 'S0025' in rejson:
                                union_dic['bank.third_party_payment_money'] = float(rejson['S0025'])
                            if 'S0026' in rejson:
                                union_dic['bank.third_party_payment_money_6'] = float(rejson['S0026'])
                            if 'S0027' in rejson:
                                union_dic['bank.third_party_payment_money_12'] = float(rejson['S0027'])
                            if 'S0028' in rejson:
                                union_dic['bank.third_party_payment_num'] = float(rejson['S0028'])
                            if 'S0029' in rejson:
                                union_dic['bank.third_party_payment_num_6'] = float(rejson['S0029'])
                            if 'S0030' in rejson:
                                union_dic['bank.third_party_payment_num_12'] = float(rejson['S0030'])
                            if 'S0031' in rejson:
                                union_dic['bank.third_party_payment_medium_money'] = float(rejson['S0031'])
                            if 'S0032' in rejson:
                                union_dic['bank.third_party_payment_medium_money_6'] = float(rejson['S0032'])
                            if 'S0033' in rejson:
                                union_dic['bank.third_party_payment_medium_money_12'] = float(rejson['S0033'])
                            if 'S0034' in rejson:
                                union_dic['bank.third_party_payment_medium_num'] = float(rejson['S0034'])
                            if 'S0035' in rejson:
                                union_dic['bank.third_party_payment_medium_num_6'] = float(rejson['S0035'])
                            if 'S0036' in rejson:
                                union_dic['bank.third_party_payment_medium_num_12'] = float(rejson['S0036'])
                            if 'S0037' in rejson:
                                union_dic['bank.third_party_payment_medium_odd_money'] = float(rejson['S0037'])
                            if 'S0038' in rejson:
                                union_dic['bank.third_party_payment_medium_odd_money_6'] = float(rejson['S0038'])
                            if 'S0039' in rejson:
                                union_dic['bank.third_party_payment_medium_odd_money_12'] = float(rejson['S0039'])
                            if 'S0040' in rejson:
                                union_dic['bank.third_party_payment_medium_odd_num'] = float(rejson['S0040'])
                            if 'S0041' in rejson:
                                union_dic['bank.third_party_payment_medium_odd_num_6'] = float(rejson['S0041'])
                            if 'S0042' in rejson:
                                union_dic['bank.third_party_payment_medium_odd_num_12'] = float(rejson['S0042'])
                            if 'S0348' in rejson:
                                union_dic['bank.pre_authorization_withhold_money'] = float(rejson['S0348'])
                            if 'S0349' in rejson:
                                union_dic['bank.pre_authorization_withhold_money_6'] = float(rejson['S0349'])
                            if 'S0350' in rejson:
                                union_dic['bank.pre_authorization_withhold_money_12'] = float(rejson['S0350'])
                            if 'S0351' in rejson:
                                union_dic['bank.pre_authorization_withhold_num'] = float(rejson['S0351'])
                            if 'S0352' in rejson:
                                union_dic['bank.pre_authorization_withhold_num_6'] = float(rejson['S0352'])
                            if 'S0353' in rejson:
                                union_dic['bank.pre_authorization_withhold_num_12'] = float(rejson['S0353'])
                            if 'S0491' in rejson:
                                union_dic['bank.2011_wedding_celebration_consume_kind'] = rejson['S0491']
                            if 'S0662' in rejson:
                                union_dic['bank.2011_wedding_celebration_consume'] = rejson['S0662']
                            if 'S0674' in rejson:
                                transaction_time = rejson['S0674']
                                # print 'S0674', transaction_time
                                if transaction_time != 'NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.get_married_duration_12'] = \
                                        int(round(1.0 * (lately_time_date - transaction_time_date).days / 30))
                            if 'S0494' in rejson:
                                transaction_time = rejson['S0494']
                                # print 'S0494',transaction_time
                                if transaction_time != 'NA':
                                    transaction_time_date = datetime.datetime.strptime(transaction_time, "%Y%m%d")
                                    union_dic['bank.pregnancy_pre_production_duration_12'] = \
                                        int(round(1.0 * (lately_time_date - transaction_time_date).days / 30))


        except Exception, e:
            # print phn
            JKLOG.Error([('e', 'get cardsStr information failue'),('phone', phn),  ('exp type', str(e))])
            # print union_dic
            return union_dic

        # print '222',len(union_dic)
        # for fea in union_dic:
        #     if fea not in iddic:
        #         print fea
        return union_dic

        # for fea in union_dic:
        #
        #     # if fea in feadic:
        #     #     wfp.write(feadic[fea]+','+fea+','+str(union_dic[fea])+'\n')
        # print union_dic

    def get_all_feature(self,file_name):
        cf = self.get_mg_conn_yellow(dconf.mg_coll_fea)

        init_file = '/home/xuyonglong/feature/working/experiment/201705/sample/'+file_name
        # outfile = 'featuretest'
        # wfp = open(outfile, 'w')
        # out_file='test'
        # wfp=open(out_file,'w')
        userdic = {}
        with open(init_file, 'r')as fp:
            for line in fp:
                linelist = line.strip().split(',')
                userdic[linelist[0]] = 0
        num = 0
        # print userdic.keys()[:5]
        for phn in userdic:
            # phn='18519509565'
            rs = cf.find({'_id': phn})
            if rs.count() > 0:
                if 'bank' in rs:
                    if 'evection_num_12' in rs['bank']:
                        continue
            if not rs or rs.count() == 0:
                # continue
                # print rs
                cf.insert({'_id': phn})
                # continue
            union_dic=self.get_union_fea(phn)
            cf.update({'_id': phn}, {'$set': union_dic})
            # break

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
                print num
                time.sleep(0.5)
                # break

    def __del__(self):
        self.conn.close()


if __name__ == '__main__':
    a = AllfeatureFind()
    JKLOG.Initialize('111', fname="dltest", log_type=1, jk_log_level="efil",
                     scribe_ip="127.0.0.1", scribe_port=1464, ntrvl=1,
                     backcount=7)
    p1 = multiprocessing.Process(target=a.get_all_feature, args=('score_916_now_pass',))
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
