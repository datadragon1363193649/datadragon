#! /usr/bin/env python
# -*- encoding: utf-8 -*-

import sys
import os

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath = os.path.split(os.path.realpath(_abs_path))[0]
apath = os.path.split(os.path.realpath(apath))[0]
apath = os.path.split(os.path.realpath(apath))[0]
apath = os.path.split(os.path.realpath(apath))[0]
# print apath
sys.path.append(apath)
# import web
import math
from bin.api.model.risk.user_info import CardId, CreditTask, UnioznpayTask, \
    BehaviorTask, RiskUser, Source_type, Contact,UserMerchant,AuthFace,\
    PersonInfo,WorkInfo,UserCard,CreditChain
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
from sqlalchemy import and_

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
        self.Session = sessionmaker(bind=self.db_engine)
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
        self.tongdun_feature_name=dconf.tongdun_feature_name

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
            self.calc_feature_jk_app(phn, v)
            s = self.calc_score(self.jk_app_model[v])
        elif app_type == 1:
            self.calc_feature_jk_h5(phn, v)
            s = self.calc_score(self.jk_h5_model[v])
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
        self.split_feature_ohe(phn, self.jk_app_bins[v])

    def calc_feature_jk_h5(self, phn, v):
        self.feature = []
        f1 = self.get_jk_basic_fea(phn)
        self.feature += f1
        f2 = self.get_comm_fea(phn)
        self.feature += f2
        self.split_feature_ohe(phn, self.jk_h5_bins[v])

    def get_distance(self, ti, moblie):
        types = ['1', '3', '5']
        locationlist = {}
        locationlist['1'] = -1
        locationlist['3'] = -1
        locationlist['5'] = -1
        for j in types:
            try:
                callmonconn = self.get_mg_connlbs(dconf.mg_collectionlbs)
                rs = callmonconn.find({'_id': moblie + '|' + str(ti) + '|' + str(j)})
                if rs.count() == 0:
                    locationlist[j] = [float(0), float(0)]
                else:
                    rsl = list(rs)
                    locationlist[j] = [float(rsl[0]['location']['coordinates'][0]),
                                       float(rsl[0]['location']['coordinates'][1])]
                    # print locationlist[j]
            except:
                JKLOG.Error([('e', 'get basic distance failue'), ('exp type', str(e))])
                locationlist[j] = [0.0, 0.0]
        juzhumat = locationlist['1']
        gongzuomat = locationlist['3']
        renzhengmat = locationlist['5']
        # print juzhumat,gongzuomat,renzhengmat
        return juzhumat, gongzuomat, renzhengmat

    def Distance2(self, lat1, lng1, lat2, lng2):  # 第二种计算方法
        radlat1 = radians(lat1)
        radlat2 = radians(lat2)
        a = radlat1 - radlat2
        b = radians(lng1) - radians(lng2)
        s = 2 * asin(sqrt(pow(sin(a / 2), 2) + cos(radlat1) * cos(radlat2) * pow(sin(b / 2), 2)))
        earth_radius = 6378.137
        s = s * earth_radius
        if s < 0:
            return -s
        else:
            return s

    def get_jk_basic_fea(self, phn):
        # phn = '18730335003'
        # print phn
        # 获取基本特征,比如性别／年龄等不需要计算的特征
        session = self.Session()
        re = None
        try:
            re = session.query(CardId.sex, CardId.age).join(
                RiskUser, CardId.userId == RiskUser.id).filter(
                RiskUser.mobile == phn).first()
        except Exception, e:
            JKLOG.Error([('e', 'get basic info failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get basic info failue, no info'), ('phone', phn)])
            return 0, 10  # 默认为男 10岁
        gender = 0
        age = 10
        try:
            gender = re.sex
            age = int(re.age)
        except:
            pass
        if gender == '男':
            gender = 0
        elif gender == '女':
            gender = 1
        else:
            gender = 2
        # print gender, age
        session.close()
        # print gender,age
        return gender, age

    def get_comm_fea(self, phn):
        # 获取通讯录／通讯详单的特征
        contacts = []
        call_details = {}
        c = self.get_mg_conn(dconf.mg_collection)
        cinfo = c.find_one({'_id': phn})
        if not cinfo:
            JKLOG.Info([('find', 'mongodb'), ('phone', phn), ('info', 'no phn')])
            f = []
            for i in range(0, 39):
                f.append(-1)
            return f
        if 'contacts' in cinfo:
            contacts = cinfo['contacts']
        else:
            JKLOG.Info([('get contacts', phn), ('error', 'no contacts')])
        if 'call_details' in cinfo:
            call_details = cinfo['call_details']
        else:
            JKLOG.Info([('get call_details', phn), ('error', 'no call_details')])
        return self.get_comm_contacts_call_details_fea(phn, contacts, call_details)

    def get_comm_contacts_call_details_fea(self, phn, contacts, call_details):
        f = []
        if 'call_info' not in call_details:
            JKLOG.Info([('data', 'call_details'), ('phone', phn), ('info', 'no call_info')])
            for i in range(0, 39):
                f.append(-1)
            return f
        try:
            f.append(self.fea_open_time(call_details['innet_date']))
        except Exception, e:
            JKLOG.Info([('phone', phn), ('call_details error', str(e))])
            f.append(1)  # 如果出现异常 设为1个月
        name_stat, sens, family, job, srv, phn_sum, phns = self.pro_contacts(contacts)
        f.append(phn_sum)
        # 训练数据集时注释掉，线上时用
        # cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # anchor1 = self.before_some_days(cur_time, 30)
        # anchor2 = self.before_some_days(cur_time, 60)
        first_call_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        anchor1 = self.before_some_days(first_call_time, 30)
        anchor2 = self.before_some_days(first_call_time, 60)
        try:
            max_comm_plac = call_details['max_comm_plac']
            first_call_time = call_details['call_info'][0]['start_time']
            anchor1 = self.before_some_days(first_call_time, 30)
            anchor2 = self.before_some_days(first_call_time, 60)
        except Exception, e:
            JKLOG.Error([('data', 'call_details'), ('phone', phn), ('error', str(e))])

            # raise HttpJsonError(700, 'Data Error')
        call_stat = {}
        # phn: [locate_call_num, remote_call_num, call_time_length]
        # 一个手机号的locate_call_num 和 remote_call_num 一定只有一个> 0
        time_seg = [[0, 0, 0], [0, 0, 0]]  # [new:早 中 晚, old:早 中 晚]
        time_len = [[0, 0], [0, 0]]  # [new:通话长度和 通话手机号和, old: 通话长度和 通话手机号和]
        time_frq = [{}, {}]  # 通话频率
        name_cate = [0, 0, 0, 0]  #
        cantact_person = [set(), set()]  # 两个月的通话手机号
        for c in call_details['call_info']:
            try:
                phn = c['phone_num']
                call_stat.setdefault(phn, [0, 0, 0])
                cl = int(round(float(c['comm_time']) / 60))
                call_stat[phn][2] += cl
                if c['comm_type'].find(u'本地') or c['comm_plac'] == max_comm_plac:
                    # 本地
                    call_stat[phn][0] += 1
                else:
                    call_stat[phn][1] += 1
                st = c['start_time']
                d = st.split(' ')[0]
                ts = self.get_time_seg(st)
                if st > anchor1:  # 最近一个月
                    time_seg[0][ts] += 1
                    time_len[0][0] += cl
                    time_len[0][1] += 1
                    time_frq[0].setdefault(d, 0)
                    time_frq[0][d] += 1
                    cantact_person[0].add(phn)
                elif st > anchor2:
                    time_seg[1][ts] += 1
                    time_len[1][0] += cl
                    time_len[1][1] += 1
                    time_frq[1].setdefault(d, 0)
                    time_frq[1][d] += 1
                    cantact_person[1].add(phn)
                else:
                    break
                if phn in sens:
                    name_cate[0] += 1
                elif phn in family:
                    name_cate[1] += 1
                elif phn in job:
                    name_cate[2] += 1
                elif phn in srv:
                    name_cate[3] += 1
            except Exception, e:
                JKLOG.Info([('data', 'call_details'), ('phone', phn), ('error', str(e))])
                continue
        time_frq[0] = time_frq[0].values() + [0]  # 避免为空集
        time_frq[1] = time_frq[1].values() + [0]
        live_contact_person_count = 0
        name_call_fea = [0, 0, 0, 0, 0, 0, 0, 0, 0]
        locate_remote_count = [0, 0, 0, 0]
        phnlist=[]
        for k, v in call_stat.items():
            if len(k)>0:
                if k.count('*')*1.0/len(k)>0.5:
                    continue
            else:
                continue
            phnlist.append(k)
            if v[0] > v[1]:
                locate_remote_count[0] += 1
                locate_remote_count[2] += v[0]
            else:
                locate_remote_count[1] += 1
                locate_remote_count[3] += v[1]
            vs = v[0] + v[1]
            if vs > 10:
                live_contact_person_count += 1
            if k in phns:
                if vs > 10:
                    name_call_fea[0] += 1  # name_frq_call_count
                    name_call_fea[1] += vs  # name_frq_call_num
                    name_call_fea[2] += v[2]  # name_frq_call_time
                else:
                    name_call_fea[3] += 1  # name_call_count
                    name_call_fea[4] += vs  # name_call_num
                    name_call_fea[5] += v[2]  # name_call_time
            else:
                name_call_fea[6] += 1  # no_name_call_count
                name_call_fea[7] += vs  # no_name_call_num
                name_call_fea[8] += v[2]  # no_name_call_time

        rltn_fea = self.get_rltn_fea(phnlist)
        f += [
                 1.0 * locate_remote_count[0] / (locate_remote_count[1] + 1),
                 1.0 * locate_remote_count[2] / (locate_remote_count[3] + 1),
                 sum(time_seg[0]) - sum(time_seg[1]),
                 time_seg[0][0] - time_seg[0][1],
                 time_seg[0][1] - time_seg[0][2],
                 time_seg[1][0] - time_seg[1][1],
                 time_seg[1][1] - time_seg[1][2],
                 (1.0 * time_len[0][0] / (time_len[0][1] + 1) -
                  1.0 * time_len[1][0] / (time_len[1][1] + 1)),
                 max(time_frq[0]) - max(time_frq[1]),
                 sum(time_frq[0]) / 4.0 - sum(time_frq[1]) / 4.0,
                 len(cantact_person[0] - cantact_person[1]),
                 1.0 * live_contact_person_count / (phn_sum + 1),
                 name_stat,
                 len(sens),
                 name_cate[0],
                 len(family),
                 name_cate[1],
                 len(job),
                 name_cate[2],
                 len(srv),
                 name_cate[3]] + name_call_fea + rltn_fea
        return f

    def get_rltn_fea(self, phns):
        # 获取手机号的关系特征
        rltn_one = [0, 0, 0]  # balck pass refuse 1度关系
        rltn_two = [0, 0, 0, 0]  # pass pass_contact refuse refuse_contact 2度关系

        rltn_two_p = [set(), set()]
        self.all_cinfo = []
        """
        self.lock = threading.Lock()
        def get_all_cinfo(phns_part):
            c = self.get_mg_conn()
            tmp_info = c.find({'_id': {'$in': phns_part}}, ['is_black', 'is_pass', 'is_refuse',
                                                            'contacted_pass_user',
                                                            'contacted_refuse_user'])
            if self.lock.acquire():
                self.all_cinfo += tmp_info
                self.lock.release()
        ti = 0
        t = []
        tar_len = len(phns)
        if tar_len > 4900: tar_len = 4900 # mongodb连接池默认100 不能超过100个连接
        for i in range(0, tar_len, 50):
            ti += 1
            z = threading.Thread(target=get_all_cinfo,args=(phns[i:i+50],))
            t.append(z)
        for i in range(ti):
            t[i].start()
        for i in range(ti):
            t[i].join()
        """
        c = self.get_mg_conn(dconf.mg_coll_relation)
        for i in range(0, len(phns), 50):
            phns_part = phns[i:i + 50]
            tmp_info = c.find({'_id': {'$in': phns_part}}, ['is_black', 'is_pass', 'is_refuse',
                                                            'contacted_pass_user',
                                                            'contacted_refuse_user'])
            self.all_cinfo += tmp_info
        for ci in self.all_cinfo:
            if 'is_black' in ci and ci['is_black']:
                rltn_one[0] += 1
            if 'is_pass' in ci and ci['is_pass']:
                rltn_one[1] += 1
            if 'is_refuse' in ci and ci['is_refuse']:
                rltn_one[2] += 1
            if 'contacted_pass_user' in ci:
                cpu = ci['contacted_pass_user']
                tmp_p_set = set()
                rltn_two[1] += len(cpu)
                try:
                    tmp_p_set = set(cpu)
                except:
                    pass
                rltn_two_p[0] |= tmp_p_set
            if 'contacted_refuse_user' in ci:
                cru = ci['contacted_refuse_user']
                rltn_two[3] += len(cru)
                tmp_r_set = set()
                try:
                    tmp_r_set = set(cru)
                except:
                    pass
                rltn_two_p[1] |= tmp_r_set
        rltn_two[0] = len(rltn_two_p[0])
        rltn_two[2] = len(rltn_two_p[1])
        rltn_two[1] = float(rltn_two[1]) / (rltn_two[0] + 1)
        rltn_two[3] = float(rltn_two[3]) / (rltn_two[2] + 1)
        return rltn_one + rltn_two

    def fea_open_time(self, open_time):
        open_time = datetime.datetime.strptime(open_time, "%Y-%m-%d %H:%M:%S")
        cur_time = datetime.datetime.now()
        open_time_length = int(round(1.0 * (cur_time - open_time).days / 30))
        return open_time_length

    def before_some_days(self, d, delta):
        return (datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S") -
                datetime.timedelta(days=delta)).strftime("%Y-%m-%d %H:%M:%S")

    def get_time_seg(self, d):
        h = datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S").hour
        ts = 0
        if h >= 7 and h <= 12:
            ts = 0
        elif h > 12 and h <= 18:
            ts = 1
        else:
            ts = 2
        return ts

    def pro_contacts(self, contacts):
        name_len = {}  # len : count
        phns = []
        sens = []  # cate tag : 0
        family = []  # cate tag : 1
        job = []  # cate tag : 2
        srv = []  # cate tag : 3
        name_stat = 1
        if not contacts:
            return (name_stat, sens, family, job, srv, len(phns), phns)
        for c in contacts:
            n = c['name']
            plist = c['phone']
            if n.strip() == '':
                continue
            nl = len(n)
            name_len.setdefault(nl, 0)
            name_len[nl] += 1
            phns += plist
            # print n
            w = self.aam.match(n)
            if w:
                cate = w[0]
                if cate == 0:
                    sens += plist
                elif cate == 1:
                    family += plist
                elif cate == 2:
                    job += plist
                else:
                    srv += plist
        name_len = sorted(name_len.items(), key=lambda x: x[0])
        tar_len_index = 1
        if len(name_len) > 3:
            tar_len_index = 3
        try:
            name_stat_1 = sum([i[1] for i in name_len[:tar_len_index]])
            name_stat_2 = sum([i[1] for i in name_len])
            name_stat = 1.0 * name_stat_1 / name_stat_2
        except Exception, e:
            # print 'namelen',name_len
            # print 'namestat',str(e)
            pass
        return (name_stat, sens, family, job, srv, len(phns), phns)

    def split_feature_ohe(self, sid, bins):
        fea = self.feature[:]
        JKLOG.Info([('x', 'feature'), ('sample_id', sid), ('origin feature', fea)])
        # print self.feature
        for i in range(len(self.feature)):
            nv = 0
            cfv = self.feature[i]
            one_hot = [0] * (len(bins[str(i)]) + 1)
            for j in bins[str(i)]:
                if cfv <= j:
                    one_hot[nv] = 1
                    break
                nv += 1
            self.feature[i] = one_hot
            fea[i] = nv
        # print self.feature
        self.feature = reduce(lambda x, y: x + y, self.feature)
        # log
        JKLOG.Info([('x', 'feature'), ('sample_id', sid), ('feature after bins', fea)])
        JKLOG.Info([('x', 'feature'), ('sample_id', sid), ('end feature', self.feature)])

    def store_feature(self, phn):
        c = self.get_mg_conn(dconf.mg_coll_feature)
        fea = {}
        for fi, f in enumerate(self.feature):
            fn = self.feature_schema[fi]
            fea[fn] = f
        if c.find_one({'_id': phn}) is None:
            c.insert({'_id': phn, 'feature': fea})
        else:
            c.update({'_id': phn}, {'$set': {'feature': fea}})
        JKLOG.Info([('x', 'store feature'), ('phn', phn), ('feature', fea)])

    def get_contact_yellow_number(self, contectsre):
        bankmocnt = 0
        cuishoumocnt = 0
        # print 'self.bank_yellowpage',self.bank_yellowpage
        # print len(self.bank_yellowpage)
        for c in contectsre:
            try:
                plist = c['phone']
                for p in plist:
                    p = str(p)
                    # print p

                    if p in self.bank_yellowpage:
                        # print 'bank_yellowpage',bankmocnt
                        bankmocnt += 1
                    if p in self.collection_yellowpage:
                        # print 'collection_yellowpage',cuishoumocnt
                        cuishoumocnt += 1
            except Exception, e:
                JKLOG.Info([('phone', plist), ('yellow_number error', str(e))])
                continue
        return bankmocnt, cuishoumocnt


    def get_linkman_yellow_number(self, refo):
        try:
            # 处理联系人
            # begincon=datetime.datetime.now()
            # print 'refo',refo['contacts']
            bankmocnt, collectionmocnt = self.get_contact_yellow_number(refo['contacts'])
            # endcon = datetime.datetime.now()
            # print 'contacts',endcon-begincon
            # print '处理联系人',bankmocnt,collectionmocnt
            # begindet = datetime.datetime.now()
            bankmocnts, collectionmocnts, bankmaxcnts, collectionmaxcnts = \
                self.get_call_yellow_number(refo['call_details'])
            # enddet = datetime.datetime.now()
            # print 'call_details', enddet - begindet
            # print bankmocnts, collectionmocnts, bankmaxcnts, collectionmaxcnts
            lianxiinfor = [bankmocnt, collectionmocnt, bankmocnts,
                           collectionmocnts, bankmaxcnts, collectionmaxcnts]
            # print lianxiinfor
            # print lianxiinfor
            return lianxiinfor
        except Exception, e:
            JKLOG.Info([('yellow_number error', str(e))])
            return [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0]

    def get_get_bankAndcollection_phone(self, yellowpage):
        try:
            c = self.get_mg_conn_yellow(dconf.mg_coll_relationy)
            re = c.find({yellowpage: {'$exists': True}})
            # print re.count()
            rsl = list(re)
            phonedic = {}
            for i in rsl:
                # print i
                phonedic[str(i['_id'])] = 0
            return phonedic
        except Exception, e:
            JKLOG.Info([('yellowpage error', str(e))])
            # phonelist.append(str(i['_id']))

    def get_bankAndcollection_yellowpage(self):
        self.bank_yellowpage = self.get_get_bankAndcollection_phone('is_bank_yellowpage')

        # print 'bank_yellowpage',self.bank_yellowpage
        # print len(self.bank_yellowpage)
        # print type(self.bank_yellowpage[0])
        self.collection_yellowpage = self.get_get_bankAndcollection_phone('is_collection_yellowpage')
        # print 'collection_yellowpage',self.collection_yellowpage

    def get_diantance_yellowpage_fea(self, uid,phn):
        # phn = '15972094386'
        session = self.Session()
        re = None
        try:
            re = session.query(CreditTask.id).filter(
                CreditTask.userId == uid).first()
            # print re
        except Exception, e:
            JKLOG.Error([('e', 'get basic distance yellow failue'), ('exp type', str(e))])
        if re is None:
            JKLOG.Info([('e', 'get basic distance yellow failue, no distance yellow'), ('phone', phn)])
            return [-1, -1, -1]
        # print re.id
        c = self.get_mg_conn(dconf.mg_collection)
        cinfo = c.find_one({'_id': phn})
        if cinfo is None:
            JKLOG.Info([('find', 'mongodb'), ('phone', phn), ('info', 'no phn')])
            return [-1, -1, -1]
        if 'call_details' in cinfo and 'contacts' in cinfo:
            # print cinfo
            # beginlian = datetime.datetime.now()
            # linkmaninformation = self.get_linkman_yellow_number(cinfo)
            # endlian = datetime.datetime.now()
            # print 'linkman',endlian-beginlian
            # begindis = datetime.datetime.now()

            # 临时注释，跑黄页数据需要

            juzhumat, gongzuomat, renzhengmat = [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]
            if re.id != None:
                # print 're.id, phn',re.id, phn
                juzhumat, gongzuomat, renzhengmat = self.get_distance(re.id, phn)
            jg = self.Distance2(juzhumat[0], juzhumat[1], gongzuomat[0], gongzuomat[1])
            jr = self.Distance2(juzhumat[0], juzhumat[1], renzhengmat[0], renzhengmat[1])
            gr = self.Distance2(gongzuomat[0], gongzuomat[1], renzhengmat[0], renzhengmat[1])
            # enddis = datetime.datetime.now()
            # print 'distance', enddis - begindis
            userinfromiation = [jg, jr, gr]
            # print userinfromiation
            # userinfromiation.extend(linkmaninformation)

            session.close()
            return userinfromiation
        else:
            JKLOG.Info([('get call_details', phn), ('error', 'no call_details')])
            session.close()
            return [-1, -1, -1]

    def max_no_communicate(self, ud):

        maxdays = -1
        if 'call_info' not in ud:
            return maxdays
        callinfo = ud['call_info']
        st = '2014-04-16 00:00:00'
        try:
            st = callinfo[0]['start_time']
            stleft = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")

        except Exception, e:
            JKLOG.Error([('data', 'max_call_info'), ('error', str(e))])
        try:
            for c in callinfo:
                st = c['start_time']
                if st == '':
                    continue
                stright = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                days = (stleft - stright).days
                if days > maxdays:
                    maxdays = days
                stleft = stright
        except Exception, e:
            JKLOG.Error([('data', 'max_call_info'), ('error', str(e))])
        return maxdays

    def get_maxNocom_sourcetype(self, phn):
        maxnocom = -1
        variance=-1
        phnnumfea=[-1,-1,-1,-1]
        # stype = -1
        # phn = '15774913554'
        # sessionsudai = self.Sessionsudai()
        c = self.get_mg_conn(dconf.mg_collection)
        cinfo = c.find_one({'_id': phn})
        if not cinfo:
            JKLOG.Info([('find', 'mongodb'), ('phone', phn), ('info', 'no phn')])
            phnnumfea.append(-1)
            return maxnocom,phnnumfea
        if 'call_details' in cinfo and 'contacts' in cinfo:
            maxnocom = self.max_no_communicate(cinfo['call_details'])
            phnnumfea=self.get_phn_num_fea(cinfo)
        # else:
        #     phnnumfea=[-1,-1,-1,-1]
        if 'call_details' in cinfo:
            if 'bill_info' in cinfo['call_details']:
                paylist=[]
                sum1=0.0
                sum2=0.0
                N=0
                for bi in cinfo['call_details']['bill_info']:
                    if 'callPay' in bi:
                        sum1+=float(bi['callPay'])
                        sum2+=float(bi['callPay'])**2
                        N+=1
                if N>0:
                    means=sum1/N
                    variance=sum2/N-means**2
        # print phnnumfea
        phnnumfea.append(variance)
        # print phnnumfea
        return maxnocom,phnnumfea
        # print phnnumfea,variance
        # print maxnocom
        # return maxnocom
    def get_maxNocom_sourcetype_lin(self, phn):
        maxnocom = -1
        variance=-1
        phnnumfea=[-1,-1,-1,-1,-1]
        # stype = -1
        # phn = '15774913554'
        # sessionsudai = self.Sessionsudai()
        c = self.get_mg_conn(dconf.mg_collection)
        cinfo = c.find_one({'_id': phn})
        if not cinfo:
            JKLOG.Info([('find', 'mongodb'), ('phone', phn), ('info', 'no phn')])
            phnnumfea.append(-1)
            return maxnocom,phnnumfea
        if 'call_details' in cinfo and 'contacts' in cinfo:
            # maxnocom = self.max_no_communicate(cinfo['call_details'])
            phnnumfea=self.get_phn_num_fea(cinfo)
        else:
            phnnumfea=[-1,-1,-1,-1,-1]
        if 'call_details' in cinfo:
            if 'bill_info' in cinfo['call_details']:
                paylist=[]
                sum1=0.0
                sum2=0.0
                N=0
                for bi in cinfo['call_details']['bill_info']:
                    if 'callPay' in bi:
                        sum1+=float(bi['callPay'])
                        sum2+=float(bi['callPay'])**2
                        N+=1
                if N>0:
                    means=sum1/N
                    variance=sum2/N-means**2
        # print phnnumfea
        phnnumfea.append(variance)
        # print phnnumfea
        return maxnocom,phnnumfea
        # print phnnumfea,variance
        # print maxnocom
        # return maxnocom


    def get_source_type(self, uid,session):
        source_type=-1
        register_date=-1
        channel_source_type=-1
        last_login_client_type=-1
        # session = self.Session()
        try:
            re=session.query(Source_type.sourceType,Source_type.createDate,
                               Source_type.channelSource,Source_type.lastLoginClient
                             ).join(UserMerchant,Source_type.id == UserMerchant.merchantUserId
                                    ).filter(UserMerchant.userId == int(uid)).first()
            # re=session.query(UserMerchant.merchantUserId
            #                         ).filter(UserMerchant.userId == int(uid)).first()
            # print 'test'
            # print re.merchantUserId,type(re.merchantUserId)
            # print int(re.merchantUserId),type(int(re.merchantUserId))
        except Exception, e:
            JKLOG.Error([('e', 'get Source_type failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
            # session.close()
            return source_type, register_date, \
                   channel_source_type, last_login_client_type
        if re is None:
            JKLOG.Info([('e', 'this Source_type fea is not in sudaibear.uc_users'), ('userid', uid)])
            # session.close()
            return source_type, register_date, \
                   channel_source_type, last_login_client_type
        try:
            source_type = int(re.sourceType)
            register_date=re.createDate
            channel_source_type=re.channelSource
            last_login_client_type=re.lastLoginClient
        except:
            pass
        if source_type == 4 or source_type == 5:
            source_type = 2
        # session.close()
        # print 'sourcetype'
        # print source_type,register_date,\
        #        channel_source_type,last_login_client_type,type(register_date)
        return source_type,register_date,\
               channel_source_type,last_login_client_type

    def get_contact_vip(self, uid):
        session = self.Session()
        try:
            re = session.query(Contact.contactMobile1, Contact.contactMobile2)\
                .filter(Contact.userId == int(uid)).first()
        except Exception, e:
            JKLOG.Error([('e', 'get vip contact failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
            return ()
        session.close()
        return re

    def get_face_fea(self,uid,session):
        face_verification_confidence=-1
        face_verification_num=-1
        identity_face_verification_confidence=-1
        face_create_date=-1
        face_update_date=-1
        # session = self.Session()
        re = None
        try:
            re = session.query(AuthFace.face_verification_confidence,
                               AuthFace.face_verification_num,
                               AuthFace.identity_face_verification_confidence,
                               AuthFace.create_date,
                               AuthFace.update_date).filter(
                AuthFace.user_id == int(uid)).first()
        except Exception, e:
            JKLOG.Error([('e', 'get AuthFace user_id  failue'), ('exp type', str(e))])
            return face_verification_confidence, \
                   face_verification_num, \
                   identity_face_verification_confidence, \
                   face_create_date, \
                   face_update_date
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get AuthFace fea failue'), ('userid', uid)])
            return face_verification_confidence, \
                   face_verification_num, \
                   identity_face_verification_confidence, \
                   face_create_date, \
                   face_update_date
        try:
            face_verification_confidence = re.face_verification_confidence
            face_verification_num = re.face_verification_num
            identity_face_verification_confidence = \
                re.identity_face_verification_confidence
            face_create_date = re.create_date,
            face_create_date=face_create_date[0]
            face_update_date = re.update_date
        except:
            pass
        # session.close()
        # print 'face'
        # print face_verification_confidence,\
        #        face_verification_num,\
        #        identity_face_verification_confidence,\
        #        face_create_date,\
        #        face_update_date,type(face_create_date),type(face_update_date)
        return face_verification_confidence,\
               face_verification_num,\
               identity_face_verification_confidence,\
               face_create_date,\
               face_update_date
    def get_person_fea(self,uid,session):
        degree=-1
        marital_status=-1
        # session = self.Session()
        re = None
        try:
            re = session.query(PersonInfo.degree,
                               PersonInfo.maritalStatus).filter(
                PersonInfo.userId == int(uid)).first()
        except Exception, e:
            JKLOG.Error([('e', 'get PersonInfo userId  failue'), ('exp type', str(e))])
            return degree,marital_status
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get PersonInfo fea failue'), ('userid', uid)])
            return degree, marital_status
        try:
            degree = re.degree
            marital_status = re.maritalStatus
        except:
            pass
        # session.close()
        # print 'person'
        # print degree,marital_status
        return degree,marital_status
    def get_work_fea(self,uid,session):
        industry=-1
        job_title=-1
        month_income=-1
        # session = self.Session()
        re = None
        try:
            re = session.query(WorkInfo.industry,
                               WorkInfo.jobTitle,
                               WorkInfo.monthlyIncome).filter(
                WorkInfo.userId == int(uid)).first()
        except Exception, e:
            JKLOG.Error([('e', 'get WorkInfo userId  failue'), ('exp type', str(e))])
            return industry,job_title,month_income
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get WorkInfo fea failue'), ('userid', uid)])
            return industry,job_title, month_income
        try:
            industry = re.industry
            job_title = re.jobTitle
            month_income = re.monthlyIncome
        except:
            pass
        # session.close()
        # print 'work'
        # print industry,job_title, month_income
        return industry,job_title, month_income
    def get_user_card_fea(self,uid,session):
        first_attestation_date=-1
        # session = self.Session()
        re = None
        try:
            re = session.query(UserCard.createDate).filter(
                and_(UserCard.userId == int(uid),UserCard.product=='payday')).first()
        except Exception, e:
            JKLOG.Error([('e', 'get UserCard userId  failue'), ('exp type', str(e))])
            return first_attestation_date
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get UserCard fea failue'), ('userid', uid)])
            return first_attestation_date
        try:
            first_attestation_date = re.createDate
        except:
            pass
        # session.close()
        return first_attestation_date
    def get_credit_chain_fea(self,uid,session):
        submit_attestation_date=-1
        # session = self.Session()
        re = None
        try:
            re = session.query(CreditChain.createDate).filter(
                CreditChain.userId == int(uid),CreditChain.pattern=='payday_basic' ).first()
        except Exception, e:
            JKLOG.Error([('e', 'get CreditChain userId  failue'), ('exp type', str(e))])
            return submit_attestation_date
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get CreditChain fea failue'), ('userid', uid)])
            return submit_attestation_date
        try:
            submit_attestation_date = re.createDate
        except:
            pass
        # session.close()
        return submit_attestation_date
    def get_contact_feature(self, uid):
        vip_pass_count = 0
        vip_other_vip_count = 0
        contactMobile = self.get_contact_vip(uid)
        if contactMobile is None:
            return vip_pass_count, vip_other_vip_count
        else:
            c = self.get_mg_conn(dconf.mg_collection)
            for m in contactMobile:
                if m is not None:
                    cinfo = c.find_one({'_id': m})
                    if cinfo is None:
                        continue
                    if 'is_pass' in cinfo:
                        vip_pass_count += 1
                    if 'contacted_important_user' in cinfo:
                        tmp = cinfo['contacted_important_user']
                        if len(tmp) >= 1:
                            vip_other_vip_count += len(tmp) - 1
            return vip_pass_count, vip_other_vip_count
    def get_cardid_fea(self, uid,session):
        gender = -1
        age = -1
        valid_End_Date = -1

        re = None
        try:
            re = session.query(CardId.sex, CardId.age, CardId.validEndDate).filter(
                CardId.userId == int(uid)).first()
        except Exception, e:
            JKLOG.Error([('e', 'get basic info failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get basic info failue, no info'), ('uid', uid)])
            return 0, 10  # 默认为男 10岁

        try:
            gender = re.sex
            age = int(re.age)
            valid_End_Date = re.validEndDate
            valid_End_Date = datetime.datetime.strptime(str(valid_End_Date), '%Y-%m-%d')
        except:
            pass
        if gender == '男':
            gender = 0
        elif gender == '女':
            gender = 1
        else:
            gender = 2
        # print gender, age
        # print 'aeg'
        # print gender,age, valid_End_Date,type(valid_End_Date)
        return gender, age, valid_End_Date
    def get_app_fea(self,uid):
        session = self.Session()
        gender, \
        age, \
        valid_End_Date=self.get_cardid_fea(uid,session)

        source_type, \
        register_date, \
        channel_source_type, \
        last_login_client_type=self.get_source_type(uid,session)

        face_verification_confidence,\
        face_verification_num,\
        identity_face_verification_confidence,\
        face_create_date,\
        face_update_date=self.get_face_fea(uid,session)

        industry, \
        job_title, \
        month_income=self.get_work_fea(uid,session)

        degree,\
        marital_status=self.get_person_fea(uid,session)
        first_attestation_date=self.get_user_card_fea(uid,session)
        submit_attestation_date = self.get_credit_chain_fea(uid,session)
        # print first_attestation_date,submit_attestation_date,\
        #     type(first_attestation_date),type(submit_attestation_date)
        # print (submit_attestation_date-first_attestation_date).seconds
        # print valid_End_Date,submit_attestation_date
        valid_submit_duration=-1
        face_duration = -1
        first_register_duration = -1
        submit_first_duration = -1
        try:
            valid_submit_duration=int(round(
                1.0 * (valid_End_Date - submit_attestation_date).days / 30))
        except Exception, e:
            JKLOG.Error([('e', 'valid_submit_duration  failue'), ('exp type', str(e))])

        try:
            face_duration = int(round(
                1.0 * (face_update_date - face_create_date).seconds / 60))
        except Exception, e:
            JKLOG.Error([('e', 'face_duration  failue'), ('exp type', str(e))])

        try:
            first_register_duration = int(round(
                1.0 * (first_attestation_date - register_date).seconds / 60))
        except Exception, e:
            JKLOG.Error([('e', 'first_register_duration  failue'), ('exp type', str(e))])

        try:
            submit_first_duration = int(round(
                1.0 * (submit_attestation_date - first_attestation_date).seconds / 60))
        except Exception, e:
            JKLOG.Error([('e', 'submit_first_duration  failue'), ('exp type', str(e))])
        # if face_update_date==-1 or face_create_date==-1:
        #     face_duration=-1
        # else:
        #     face_duration = int(round(
        #         1.0 * (face_update_date - face_create_date).seconds / 60))
        # if first_attestation_date == -1 or register_date == -1:
        #     first_register_duration = -1
        # else:
        #     first_register_duration=int(round(
        #         1.0 * (first_attestation_date - register_date).seconds / 60))
        # if submit_attestation_date == -1 or first_attestation_date == -1:
        #     submit_first_duration = -1
        # else:
        #     submit_first_duration = int(round(
        #         1.0 * (submit_attestation_date - first_attestation_date).seconds / 60))
        # print valid_submit_duration,face_duration,first_register_duration,submit_first_duration
        return gender,\
               age,\
               source_type, \
               channel_source_type, \
               last_login_client_type,\
               face_verification_confidence,\
               face_verification_num,\
               identity_face_verification_confidence, \
               industry, \
               job_title, \
               month_income,\
               degree,\
               marital_status,\
               valid_submit_duration,\
               face_duration,\
               first_register_duration,\
               submit_first_duration


    def get_all_feature(self):
        cf = self.get_mg_conn_yellow(dconf.mg_coll_fea)
        init_file = '/home/xuyonglong/feature/working/experiment/201705/sample/use_uid_phn'
        # out_file='test'
        # wfp=open(out_file,'w')
        userdic = {}
        with open(init_file, 'r')as fp:
            for line in fp:
                linelist = line.strip().split(',')
                userdic[linelist[0]] = linelist[1]

        # cf.update({'_id': phn}, {'$set': {'call': {},'bank':{},'credit_behavior':{}}})
        # self.get_bankAndcollection_yellowpage()
        num = 0
        print userdic.keys()[:10]
        print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for uid in userdic:
            # phn='15575219990'
            phn = userdic[uid]
            rs = cf.find({'_id': phn})
            # print rs['_id']
            if rs.count() > 0:
                rsl = list(rs)
                if 'app' in rsl[0]:
                    # print rsl[0]['app']
                    # if 'max_phn_day_num' in rs['call']:
                    # break
                    continue
            if not rs or rs.count() == 0:
                # continue
                # print rs
                cf.insert({'_id': phn})
            #     # continue
            # gender, age = self.get_jk_basic_fea(phn)
            # td_featurelist=self.get_td_fea(phn)
            # print td_featurelist
            # break
            # cf.update({'_id': phn}, {'$set': {'tongdun.apply_borrow_num': td_featurelist[0],
            #                                   'tongdun.identity_low_risk': td_featurelist[1],
            #                                   'tongdun.identity_medium_risk': td_featurelist[2],
            #                                   'tongdun.identity_relevance_apply_num': td_featurelist[3],
            #                                   'tongdun.apply_borrow_num_7_days': td_featurelist[4],
            #                                   'tongdun.apply_borrow_num_3': td_featurelist[5],
            #                                   'tongdun.apply_borrow_num_6': td_featurelist[6],
            #                                   'tongdun.apply_borrow_num_12': td_featurelist[7],
            #                                   'tongdun.apply_borrow_num_18': td_featurelist[8],
            #                                   'tongdun.apply_borrow_num_24': td_featurelist[9],
            #                                   'tongdun.apply_borrow_num_60': td_featurelist[10],
            #                                   'tongdun.phone_low_risk': td_featurelist[11],
            #                                   'tongdun.phone_address_true_ip_address_mismatching': td_featurelist[12],
            #                                   'tongdun.phone_medium_risk': td_featurelist[13],
            #                                   'tongdun.apply_phone_to_contacts_num_equal_greater_two': td_featurelist[14],
            #                                   'tongdun.identity_apply_many_equipment_1_days': td_featurelist[15],
            #                                   'tongdun.identity_hit_zxd_overdue_list_max_days': td_featurelist[16],
            #                                   'tongdun.apply_information_many_identity_3': td_featurelist[17],
            #                                   'tongdun.apply_ip_address_true_ip_address_mismatching': td_featurelist[18],
            #                                   'tongdun.identity_apply_many_equipment_7_days': td_featurelist[19],
            #                                   'tongdun.phone_high_risk': td_featurelist[20],
            #                                   'tongdun.identity_high_risk_area': td_featurelist[21]
            #                                   }
            #                          })
            # uid='11245965'
            # phn='13001028373'

            gender, \
            age, \
            source_type, \
            channel_source_type, \
            last_login_client_type, \
            face_verification_confidence, \
            face_verification_num, \
            identity_face_verification_confidence, \
            industry, \
            job_title, \
            month_income, \
            degree, \
            marital_status, \
            valid_submit_duration, \
            face_duration, \
            first_register_duration, \
            submit_first_duration=self.get_app_fea(uid)

            life_work_distance, \
            life_auth_distance, \
            work_auth_distance= self.get_diantance_yellowpage_fea(uid,phn)
            # print life_work_distance, \
            # life_auth_distance, \
            # work_auth_distance
            # vip_pass_count, vip_other_vip_count = self.get_contact_feature(uid)
            # print  vip_pass_count, vip_other_vip_count
            cf.update({'_id': phn}, {'$set': {'app.gender': gender,
                                              'app.age': age,
                                              'app.source_type': source_type,
                                              'app.channel_source_type': channel_source_type,
                                              'app.last_login_client_type': last_login_client_type,
                                              'app.face_verification_confidence': face_verification_confidence,
                                              'app.face_verification_num': face_verification_num,
                                              'app.identity_face_verification_confidence':
                                                  identity_face_verification_confidence,
                                              'app.industry': industry,
                                              'app.job_title': job_title,
                                              'app.month_income': month_income,
                                              'app.degree': degree,
                                              'app.marital_status': marital_status,
                                              'app.valid_submit_duration': valid_submit_duration,
                                              'app.face_duration': face_duration,
                                              'app.first_register_duration': first_register_duration,
                                              'app.submit_first_duration': submit_first_duration,
                                              'app.life_work_distance': life_work_distance,
                                              'app.life_auth_distance': life_auth_distance,
                                              'app.work_auth_distance': work_auth_distance,
                                              # 'app.vip_pass_count': vip_pass_count,
                                              # 'app.vip_other_vip_count': vip_other_vip_count,
                                              'userid':uid
                                              }
                                     })
            # break
            num += 1
            # break
            if num % 1000 == 0:
                time.sleep(0.2)
                print num
                # print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                break

    def __del__(self):
        self.conn.close()


if __name__ == '__main__':
    a = AllfeatureFind()
    JKLOG.Initialize('111', fname="dltest", log_type=1, jk_log_level="efil",
                     scribe_ip="127.0.0.1", scribe_port=1464, ntrvl=1,
                     backcount=7)
    a.get_all_feature()
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
