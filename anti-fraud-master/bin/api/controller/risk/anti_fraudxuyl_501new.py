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

    def get_call_yellow_number(self, callre):
        bankmocnt = 0
        collectionmocnt = 0
        bankmaxcnt = {}
        collectionmaxcnt = {}
        # 本地异地比例
        # bank_locate_remote_person = [0, 0]
        # cuishou_locate_remote_person = [0, 0]
        callinfo = callre['call_info']
        # print callre
        num = 0
        month = '-1'
        for ci in callinfo:
            try:
                # print ci['phone_num']
                p = ci['phone_num']
                if len(p) > 0:
                    if p.count('*') * 1.0 / len(p) > 0.5:
                        continue
                else:
                    continue

                st = ci['start_time']
                if st == '':
                    continue
                if st[6] != month:
                    # print st[6]
                    month = st[6]
                    num += 1
                    if num > 3:
                        break
                if p in self.bank_yellowpage:
                    bankmocnt += 1
                    stime = st[0:7]
                    bankmaxcnt.setdefault(stime, 0)
                    bankmaxcnt[stime] += 1
                if p in self.collection_yellowpage:
                    collectionmocnt += 1
                    stime = st[0:7]
                    collectionmaxcnt.setdefault(stime, 0)
                    collectionmaxcnt[stime] += 1
            except Exception, e:
                JKLOG.Info([('phone', p), ('yellow_number error', str(e))])

        if len(bankmaxcnt) > 0:
            bankmaxcnts = max(bankmaxcnt.values())
        else:
            bankmaxcnts = 0
        if len(collectionmaxcnt) > 0:
            collectionmaxcnts = max(collectionmaxcnt.values())
        else:
            collectionmaxcnts = 0
        return bankmocnt, collectionmocnt, bankmaxcnts, collectionmaxcnts

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

    def get_diantance_yellowpage_fea(self, phn):
        # phn = '15972094386'
        session = self.Session()
        re = None
        try:
            re = session.query(CreditTask.id).filter(
                CreditTask.userName == phn).first()
            # print re
        except Exception, e:
            JKLOG.Error([('e', 'get basic distance yellow failue'), ('exp type', str(e))])
        if re is None:
            JKLOG.Info([('e', 'get basic distance yellow failue, no distance yellow'), ('phone', phn)])
            return [-1, -1, -1, -1, -1, -1, -1, -1, -1]
        # print re.id
        c = self.get_mg_conn(dconf.mg_collection)
        cinfo = c.find_one({'_id': phn})
        if cinfo is None:
            JKLOG.Info([('find', 'mongodb'), ('phone', phn), ('info', 'no phn')])
            return [-1, -1, -1, -1, -1, -1, -1, -1, -1]
        if 'call_details' in cinfo and 'contacts' in cinfo:
            # print cinfo
            # beginlian = datetime.datetime.now()
            linkmaninformation = self.get_linkman_yellow_number(cinfo)
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
            userinfromiation.extend(linkmaninformation)

            session.close()
            return userinfromiation
        else:
            JKLOG.Info([('get call_details', phn), ('error', 'no call_details')])
            session.close()
            return [-1, -1, -1, -1, -1, -1, -1, -1, -1]

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
    def consumption_mysql(self, phn):
        # 近12月信用卡月消费平均额
        trade_mony_credit_12 = -1
        # 近12月博彩消费金额
        lottery_sum_12 = -1
        # 近6月有无境外消费
        consume_abroad_TF = -1
        # 信用卡还款金额
        repay_amount_credit = -1
        # 信用卡还款笔数
        repay_count_credit = -1
        # 近3月交易总金额（消费类）
        consume_amount_3 = -1
        # 近6月交易总金额（消费类）
        consume_amount_6 = -1
        # 近3月交易总笔数（消费类）
        consume_count_3 = -1
        # 近6月交易总笔数（消费类）
        consume_count_6 = -1
        # 近12月航空消费笔数
        consume_count_airline_12 = -1
        # 取现金额占交易金额比例
        withdraw_trade_ratio = -1
        # 夜交易金额
        night_trade_amount = -1
        # 夜交易笔数
        night_trade_count = -1
        # 近6月银行卡转账金额
        transfer_amount_6 = -1
        # 近12月银行卡转账金额
        transfer_amount_12 = -1
        # 近6月银行卡转账笔数
        transfer_count_6 = -1
        # 近12月银行卡转账笔数
        transfer_count_12 = -1
        # 近6月网购金额
        online_amount_6 = -1
        # 近6月网购笔数
        online_count_6 = -1
        # 境外交易笔数
        consume_abroad_count = -1
        session = self.Session()
        re = None
        try:
            re = session.query(UnioznpayTask.resultDetail).filter(
                UnioznpayTask.mobile == phn).first()
        except Exception, e:
            JKLOG.Error([('e', 'get basic consumption failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get basic consumption failue, consumption'), ('phone', phn)])
            return trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
                   repay_amount_credit, repay_count_credit, consume_amount_3, \
                   consume_amount_6, consume_count_3, consume_count_6, \
                   consume_count_airline_12, withdraw_trade_ratio, \
                   night_trade_amount, night_trade_count, \
                   transfer_amount_6, transfer_amount_12, transfer_count_6, \
                   transfer_count_12, online_amount_6, \
                   online_count_6, consume_abroad_count
        try:
            ul = json.loads(re.resultDetail)
            # print ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0629']

            if ul['code'] == 'E000000':
                # print ul['code']
                if 'S0544' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    trade_mony_credit_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0544']
                    if trade_mony_credit_12 == None:
                        trade_mony_credit_12 = -1
                if 'S0428' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    lottery_sum_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0428']
                    if lottery_sum_12 == None:
                        lottery_sum_12 = -1
                if 'S0680' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    consume_abroad_TF = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0680']
                    if consume_abroad_TF == None:
                        consume_abroad_TF = -1
                if 'S0670' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    repay_amount_credit = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0670']
                    if repay_amount_credit == None:
                        repay_amount_credit = -1
                if 'S0671' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    repay_count_credit = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0671']
                    if repay_count_credit == None:
                        repay_count_credit = -1
                if 'S0549' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    consume_amount_3 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0549']
                    if consume_amount_3 == None:
                        consume_amount_3 = -1
                if 'S0550' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    consume_amount_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0550']
                    if consume_amount_6 == None:
                        consume_amount_6 = -1
                if 'S0552' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    consume_count_3 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0552']
                    if consume_count_3 == None:
                        consume_count_3 = -1
                if 'S0553' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    consume_count_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0553']
                    if consume_count_6 == None:
                        consume_count_6 = -1
                if 'S0359' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    consume_count_airline_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0359']
                    if consume_count_airline_12 == None:
                        consume_count_airline_12 = -1
                if 'S0629' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    withdraw_trade_ratio = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0629']
                    if withdraw_trade_ratio == None:
                        withdraw_trade_ratio = -1
                    else:
                        withdn = float(withdraw_trade_ratio.split('%')[0])
                        withdraw_trade_ratio = withdn / 100
                if 'S0636' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    night_trade_amount = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0636']
                    if night_trade_amount == None:
                        night_trade_amount = -1
                if 'S0637' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    night_trade_count = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0637']
                    if night_trade_count == None:
                        night_trade_count = -1
                if 'S0331' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    transfer_amount_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0331']
                    if transfer_amount_6 == None:
                        transfer_amount_6 = -1
                if 'S0332' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    transfer_amount_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0332']
                    if transfer_amount_12 == None:
                        transfer_amount_12 = -1
                if 'S0334' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    transfer_count_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0334']
                    if transfer_count_6 == None:
                        transfer_count_6 = -1
                if 'S0335' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    transfer_count_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0335']
                    if transfer_count_12 == None:
                        transfer_count_12 = -1
                if 'S0056' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    online_amount_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0056']
                    if online_amount_6 == None:
                        online_amount_6 = -1
                if 'S0059' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    online_count_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0059']
                    if online_count_6 == None:
                        online_count_6 = -1
                if 'S0102' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                    consume_abroad_count = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0102']
                    if consume_abroad_count == None:
                        consume_abroad_count = -1
        except:
            pass
        session.close()
        return trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
               repay_amount_credit, repay_count_credit, consume_amount_3, \
               consume_amount_6, consume_count_3, consume_count_6, \
               consume_count_airline_12, withdraw_trade_ratio, \
               night_trade_amount, night_trade_count, \
               transfer_amount_6, transfer_amount_12, transfer_count_6, \
               transfer_count_12, online_amount_6, \
               online_count_6, consume_abroad_count
    def consumption(self, phn):
        # 近12月信用卡月消费平均额
        trade_mony_credit_12 = -1
        # 近12月博彩消费金额
        lottery_sum_12 = -1
        # 近6月有无境外消费
        consume_abroad_TF = -1
        # 信用卡还款金额
        repay_amount_credit = -1
        # 信用卡还款笔数
        repay_count_credit = -1
        # 近3月交易总金额（消费类）
        consume_amount_3 = -1
        # 近6月交易总金额（消费类）
        consume_amount_6 = -1
        # 近3月交易总笔数（消费类）
        consume_count_3 = -1
        # 近6月交易总笔数（消费类）
        consume_count_6 = -1
        # 近12月航空消费笔数
        consume_count_airline_12 = -1
        # 取现金额占交易金额比例
        withdraw_trade_ratio = -1
        # 夜交易金额
        night_trade_amount = -1
        # 夜交易笔数
        night_trade_count = -1
        # 近6月银行卡转账金额
        transfer_amount_6 = -1
        # 近12月银行卡转账金额
        transfer_amount_12 = -1
        # 近6月银行卡转账笔数
        transfer_count_6 = -1
        # 近12月银行卡转账笔数
        transfer_count_12 = -1
        # 近6月网购金额
        online_amount_6 = -1
        # 近6月网购笔数
        online_count_6 = -1
        # 境外交易笔数
        consume_abroad_count = -1
        # print 111
        c = self.get_mg_connlbs(dconf.mg_collection_detail)
        cinfo = c.find_one({'mobile': phn})
        # print cinfo
        if not cinfo:
            JKLOG.Info([('find', 'mongodb'), ('phone', phn), ('info', 'no phn bank')])
            return trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
                           repay_amount_credit, repay_count_credit, consume_amount_3, \
                       consume_amount_6, consume_count_3, consume_count_6, \
                       consume_count_airline_12, withdraw_trade_ratio, \
                       night_trade_amount, night_trade_count, \
                       transfer_amount_6, transfer_amount_12, transfer_count_6, \
                       transfer_count_12, online_amount_6, \
                       online_count_6, consume_abroad_count
        if 'cardsStr' not in cinfo:
            return trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
                   repay_amount_credit, repay_count_credit, consume_amount_3, \
                   consume_amount_6, consume_count_3, consume_count_6, \
                   consume_count_airline_12, withdraw_trade_ratio, \
                   night_trade_amount, night_trade_count, \
                   transfer_amount_6, transfer_amount_12, transfer_count_6, \
                   transfer_count_12, online_amount_6, \
                   online_count_6, consume_abroad_count
        # session = self.Session()
        # re = None
        # try:
        #     re = session.query(UnioznpayTask.resultDetail).filter(
        #         UnioznpayTask.mobile == phn).first()
        # except Exception, e:
        #     JKLOG.Error([('e', 'get basic consumption failue'), ('exp type', str(e))])
        #     # raise HttpJsonError(602, 'Get Info From DB Error')
        # # print re
        # if re is None:
        #     JKLOG.Info([('e', 'get basic consumption failue, consumption'), ('phone', phn)])
        #     return trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
        #            repay_amount_credit, repay_count_credit, consume_amount_3, \
        #            consume_amount_6, consume_count_3, consume_count_6, \
        #            consume_count_airline_12, withdraw_trade_ratio, \
        #            night_trade_amount, night_trade_count, \
        #            transfer_amount_6, transfer_amount_12, transfer_count_6, \
        #            transfer_count_12, online_amount_6, \
        #            online_count_6, consume_abroad_count
        try:
            ul = json.loads(cinfo['cardsStr'])
            # print len(ul)
            for uj in ul :
                # print uj
                uv=ul[uj]
                # if 'bankType' not in uv:
                #     return trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
                #            repay_amount_credit, repay_count_credit, consume_amount_3, \
                #            consume_amount_6, consume_count_3, consume_count_6, \
                #            consume_count_airline_12, withdraw_trade_ratio, \
                #            night_trade_amount, night_trade_count, \
                #            transfer_amount_6, transfer_amount_12, transfer_count_6, \
                #            transfer_count_12, online_amount_6, \
                #            online_count_6, consume_abroad_count
                if 'bankType' in uv and 'resultDetail' in uv:
                    if uv['bankType']=='1':
                        resultdetail=uv['resultDetail']
                        rejson = json.loads(resultdetail)
                        # print type(rejson)

            # print ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0629']

            # print ul['code']
            #             print rejson['S0544']
                        if 'S0544' in rejson:
                            # print rejson['S0544']
                            # print type(rejson['S0544'])
                            trade_mony_credit_12=rejson['S0544']
            #             if 'S0544' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
            #                 trade_mony_credit_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0544']
                            if trade_mony_credit_12 == None:
                                trade_mony_credit_12 = -1
                        # if 'S0428' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     lottery_sum_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0428']
                        if 'S0428' in rejson:
                            lottery_sum_12 = rejson['S0428']
                            if lottery_sum_12 == None:
                                lottery_sum_12 = -1
                        # if 'S0680' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     consume_abroad_TF = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0680']
                        if 'S0680' in rejson:
                            consume_abroad_TF = rejson['S0680']
                            if consume_abroad_TF == None:
                                consume_abroad_TF = -1
                        # if 'S0670' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     repay_amount_credit = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0670']
                        if 'S0670' in rejson:
                            repay_amount_credit = rejson['S0670']
                            if repay_amount_credit == None:
                                repay_amount_credit = -1
                        # if 'S0671' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     repay_count_credit = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0671']
                        if 'S0671' in rejson:
                            repay_count_credit = rejson['S0671']
                            if repay_count_credit == None:
                                repay_count_credit = -1
                        # if 'S0549' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     consume_amount_3 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0549']
                        if 'S0549' in rejson:
                            consume_amount_3 = rejson['S0549']
                            if consume_amount_3 == None:
                                consume_amount_3 = -1
                        # if 'S0550' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     consume_amount_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0550']
                        if 'S0550' in rejson:
                            consume_amount_6 = rejson['S0550']
                            if consume_amount_6 == None:
                                consume_amount_6 = -1
                        # if 'S0552' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     consume_count_3 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0552']
                        if 'S0552' in rejson:
                            consume_count_3 = rejson['S0552']
                            if consume_count_3 == None:
                                consume_count_3 = -1
                        # if 'S0553' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     consume_count_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0553']
                        if 'S0553' in rejson:
                            consume_count_6 = rejson['S0553']
                            if consume_count_6 == None:
                                consume_count_6 = -1
                        # if 'S0359' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     consume_count_airline_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0359']
                        if 'S0359' in rejson:
                            consume_count_airline_12 = rejson['S0359']
                            if consume_count_airline_12 == None:
                                consume_count_airline_12 = -1
                        # if 'S0629' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     withdraw_trade_ratio = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0629']
                        if 'S0629' in rejson:
                            withdraw_trade_ratio = rejson['S0629']
                            if withdraw_trade_ratio == None:
                                withdraw_trade_ratio = -1
                            else:
                                withdn = float(withdraw_trade_ratio.split('%')[0])
                                withdraw_trade_ratio = withdn / 100
                        # if 'S0636' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     night_trade_amount = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0636']
                        if 'S0636' in rejson:
                            night_trade_amount = rejson['S0636']
                            if night_trade_amount == None:
                                night_trade_amount = -1

                        # if 'S0637' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     night_trade_count = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0637']
                        if 'S0637' in rejson:
                            night_trade_count = rejson['S0637']
                            if night_trade_count == None:
                                night_trade_count = -1
                        # if 'S0331' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     transfer_amount_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0331']
                        if 'S0331' in rejson:
                            transfer_amount_6 = rejson['S0331']
                            if transfer_amount_6 == None:
                                transfer_amount_6 = -1
                        # if 'S0332' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     transfer_amount_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0332']
                        if 'S0332' in rejson:
                            transfer_amount_12 = rejson['S0332']
                            if transfer_amount_12 == None:
                                transfer_amount_12 = -1
                        # if 'S0334' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     transfer_count_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0334']
                        if 'S0334' in rejson:
                            transfer_count_6 = rejson['S0334']
                            if transfer_count_6 == None:
                                transfer_count_6 = -1
                        # if 'S0335' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     transfer_count_12 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0335']
                        if 'S0335' in rejson:
                            transfer_count_12 = rejson['S0335']
                            if transfer_count_12 == None:
                                transfer_count_12 = -1
                        # if 'S0056' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     online_amount_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0056']
                        if 'S0056' in rejson:
                            online_amount_6 = rejson['S0056']
                            if online_amount_6 == None:
                                online_amount_6 = -1
                        # if 'S0059' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     online_count_6 = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0059']
                        if 'S0059' in rejson:
                            online_count_6 = rejson['S0059']
                            if online_count_6 == None:
                                online_count_6 = -1
                        # if 'S0102' in ul['data']['unionpay_score'][0]['origin']['result']['quota']:
                        #     consume_abroad_count = ul['data']['unionpay_score'][0]['origin']['result']['quota']['S0102']
                        if 'S0102' in rejson:
                            # print rejson['S0102']
                            consume_abroad_count = rejson['S0102']
                            if consume_abroad_count == None:
                                consume_abroad_count = -1
                        # print '111'
        except:
            pass
        # print trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
        #        repay_amount_credit, repay_count_credit, consume_amount_3, \
        #        consume_amount_6, consume_count_3, consume_count_6, \
        #        consume_count_airline_12, withdraw_trade_ratio, \
        #        night_trade_amount, night_trade_count, \
        #        transfer_amount_6, transfer_amount_12, transfer_count_6, \
        #        transfer_count_12, online_amount_6, \
        #        online_count_6, consume_abroad_count
        return trade_mony_credit_12, lottery_sum_12, consume_abroad_TF, \
               repay_amount_credit, repay_count_credit, consume_amount_3, \
               consume_amount_6, consume_count_3, consume_count_6, \
               consume_count_airline_12, withdraw_trade_ratio, \
               night_trade_amount, night_trade_count, \
               transfer_amount_6, transfer_amount_12, transfer_count_6, \
               transfer_count_12, online_amount_6, \
               online_count_6, consume_abroad_count

    def credit(self, phn):
        application_platforms = -1
        application_pount = -1
        registration_platforms = -1
        registration_count = -1
        overdue_platforms = -1
        overdue_count = -1
        loanlenders_count_6 = -1
        loanlenders_count_12 = -1
        loanlenders_count = -1
        loanlenders_platforms_6 = -1
        loanlenders_platforms_12 = -1
        loanlenders_platforms = -1
        rejection_count_6 = -1
        rejection_count_12 = -1
        rejection_count = -1
        rejection_platforms_6 = -1
        rejection_platforms_12 = -1
        rejection_platforms = -1
        session = self.Session()
        re = None
        try:
            re = session.query(BehaviorTask.applicationPlatforms, BehaviorTask.applicationCount,
                               BehaviorTask.registrationPlatforms, BehaviorTask.registrationCount,
                               BehaviorTask.overduePlatforms, BehaviorTask.overdueCount,
                               BehaviorTask.loanlendersSixMonthsCount, BehaviorTask.loanlendersTwelveMonthsCount,
                               BehaviorTask.loanlendersCount, BehaviorTask.loanlendersSixMonthsPlatforms,
                               BehaviorTask.loanlendersTwelveMonthsPlatforms, BehaviorTask.loanlendersPlatforms,
                               BehaviorTask.rejectionSixMonthsCount, BehaviorTask.rejectionTwelveMonthsCount,
                               BehaviorTask.rejectionCount, BehaviorTask.rejectionSixMonthsPlatforms,
                               BehaviorTask.rejectionTwelveMonthsPlatforms, BehaviorTask.rejectionPlatforms).filter(
                BehaviorTask.mobile == phn).first()
        except Exception, e:
            JKLOG.Error([('e', 'get basic credit failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
        # print re
        if re is None:
            JKLOG.Info([('e', 'get basic credit failue, no credit'), ('phn', phn)])
            return application_platforms, application_pount, registration_platforms, registration_count, \
                   overdue_platforms, overdue_count, loanlenders_count_6, loanlenders_count_12, \
                   loanlenders_count, loanlenders_platforms_6, loanlenders_platforms_12, \
                   loanlenders_platforms, rejection_count_6, rejection_count_12, \
                   rejection_count, rejection_platforms_6, rejection_platforms_12, rejection_platforms
        # print re
        try:
            if re.applicationPlatforms is not None:
                application_platforms = int(re.applicationPlatforms)
            if re.applicationCount is not None:
                application_pount = int(re.applicationCount)
            if re.registrationPlatforms is not None:
                registration_platforms = int(re.registrationPlatforms)
            if re.registrationCount is not None:
                registration_count = int(re.registrationCount)
            if re.overduePlatforms is not None:
                overdue_platforms = int(re.overduePlatforms)
            if re.overdueCount is not None:
                overdue_count = int(re.overdueCount)
            if re.loanlendersSixMonthsCount is not None:
                loanlenders_count_6 = int(re.loanlendersSixMonthsCount)
            if re.loanlendersTwelveMonthsCount is not None:
                loanlenders_count_12 = int(re.loanlendersTwelveMonthsCount)
            if re.loanlendersCount is not None:
                loanlenders_count = int(re.loanlendersCount)
            if re.loanlendersSixMonthsPlatforms is not None:
                loanlenders_platforms_6 = int(re.loanlendersSixMonthsPlatforms)
            if re.loanlendersTwelveMonthsPlatforms is not None:
                loanlenders_platforms_12 = int(re.loanlendersTwelveMonthsPlatforms)
            if re.loanlendersPlatforms is not None:
                loanlenders_platforms = int(re.loanlendersPlatforms)
            if re.rejectionSixMonthsCount is not None:
                rejection_count_6 = int(re.rejectionSixMonthsCount)
            if re.rejectionTwelveMonthsCount is not None:
                rejection_count_12 = int(re.rejectionTwelveMonthsCount)
            if re.rejectionCount is not None:
                rejection_count = int(re.rejectionCount)
            if re.rejectionSixMonthsPlatforms is not None:
                rejection_platforms_6 = int(re.rejectionSixMonthsPlatforms)
            if re.rejectionTwelveMonthsPlatforms is not None:
                rejection_platforms_12 = int(re.rejectionTwelveMonthsPlatforms)
            if re.rejectionPlatforms is not None:
                rejection_platforms = int(re.rejectionPlatforms)

        except:
            pass
        # print [applicationPlatforms,applicationCount,registrationPlatforms,registrationCount,
        #         overduePlatforms,overdueCount,loanlendersSixMonthsCount,loanlendersTwelveMonthsCount,
        #         loanlendersCount,loanlendersSixMonthsPlatforms,loanlendersTwelveMonthsPlatforms,
        #         loanlendersPlatforms,rejectionSixMonthsCount,rejectionTwelveMonthsCount,
        #         rejectionCount,rejectionSixMonthsPlatforms,rejectionTwelveMonthsPlatforms,
        #         rejectionPlatforms]
        session.close()
        return application_platforms, application_pount, registration_platforms, registration_count, \
               overdue_platforms, overdue_count, loanlenders_count_6, loanlenders_count_12, \
               loanlenders_count, loanlenders_platforms_6, loanlenders_platforms_12, \
               loanlenders_platforms, rejection_count_6, rejection_count_12, \
               rejection_count, rejection_platforms_6, rejection_platforms_12, rejection_platforms

    def get_source_type(self, phn):
        session = self.Sessionsudai()
        try:
            re = session.query(Source_type.sourceType).filter(Source_type.mobile == phn).first()
        except Exception, e:
            JKLOG.Error([('e', 'get source_type failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
            session.close()
            return -1
        if re is None:
            JKLOG.Info([('e', 'this phone is not in sudaibear.uc_users'), ('phone', phn)])
            session.close()
            return -1
        st = -1
        try:
            st = int(re.sourceType)
        except:
            pass
        if st == 4 or st == 5:
            st = 2
        session.close()
        return st

    def get_contact_vip(self, phn):
        session = self.Session()
        try:
            re = session.query(Contact.contactMobile1, Contact.contactMobile2). \
                join(RiskUser, RiskUser.id == Contact.userId).filter(RiskUser.mobile == phn).first()
        except Exception, e:
            JKLOG.Error([('e', 'get vip contact failue'), ('exp type', str(e))])
            # raise HttpJsonError(602, 'Get Info From DB Error')
            return ()
        session.close()
        return re

    def get_contact_feature(self, phn):
        vip_pass_count = 0
        vip_other_vip_count = 0
        contactMobile = self.get_contact_vip(phn)
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
    def get_phn_num_fea(self,cinfo):
        phn_detail_dic = {}
        phn_contact_dic = {}
        # first_call_time = '2014-04-16 00:00:00'
        first_call_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # print first_call_time
        anchor1 = self.before_some_days(first_call_time, 30)
        # anchor2 = self.before_some_days(first_call_time, 60)
        call_details=cinfo['call_details']
        call_contacts=cinfo['contacts']
        for contact in call_contacts:
            try:
                for phn in contact['phone']:
                    phn_contact_dic[phn]=0
            except Exception, e:
                JKLOG.Error([('data', 'contacts'), ('no phone'),('error', str(e))])
        try:
            # max_comm_plac = call_details['max_comm_plac']
            first_call_time = call_details['call_info'][0]['start_time']
            anchor1 = self.before_some_days(first_call_time, 30)
            # print anchor1
            # anchor2 = self.before_some_days(first_call_time, 90)
        except Exception, e:
            JKLOG.Error([('data', 'call_details'), ('error', str(e))])
        # call_stat = {}
        time_frq = {}
        lately_phn_num=-1
        max_phn_num_day=-1
        user_all_num=0
        user_no_ans_num=0
        contact_num_all_num_ratio=-1
        phn_in_dic = {}
        phnlist=[]
        pattern='1.{10}'
        contact_num=0
        if 'call_info' not in call_details:
            return [-1,-1,-1,-1,-1]
        for c in call_details['call_info']:
            phnbool=-1
            try:
                phn = c['phone_num']
                if re.match(pattern,phn):
                    phnbool=1
                else:
                    phnbool=0
                st = c['start_time']
                com_mode=c['comm_mode']
                d = st.split(' ')[0]
                # ts = self.get_time_seg(st)
                if st > anchor1:  # 最近一个月
                    phn_detail_dic[phn] = 0
                    # if phn not in phndic:
                        # phndic[phn]=0
                    if com_mode=='主叫':
                        if phnbool==1:
                            time_frq.setdefault(d, {})
                            time_frq[d].setdefault(phn,0)
                    if com_mode=='被叫':
                        if phnbool==0:
                            if phn not in self.bank_operator_list:
                                    phn_in_dic[phn]=0
                            if c['comm_time']<10:
                                phnlist.append(phn)
                                user_no_ans_num+=1
                # elif st > anchor2:
                #     time_frq[1].setdefault(d, 0)
                #     time_frq[1][d] += 1
                else:
                    break
            except Exception, e:
                JKLOG.Info([('data', 'call_details'), ('function','get_phn_num_fea'),('phone', phn), ('error', str(e))])
                continue
        for phn in phn_contact_dic:
            if phn in phn_detail_dic:
                contact_num+=1
        # print phn_in_dic.keys()
        # print phnlist
        user_all_num=len(phn_in_dic)
        # print contact_num
        contact_num_all_num_ratio=contact_num*1.0/(len(phn_detail_dic)+1)
        # print time_frq
        try:
            for frq in time_frq:
                time_frq[frq]=len(time_frq[frq])
            # print time_frq
            lately_phn_num = time_frq[max(time_frq)]
            time_frq = time_frq.values() + [0]
            max_phn_num_day=max(time_frq)
        except:
            pass
        # print [lately_phn_num,user_all_num,user_no_ans_num,contact_num_all_num_ratio,max_phn_num_day]
        return [lately_phn_num,user_all_num,user_no_ans_num,contact_num_all_num_ratio,max_phn_num_day]
    def get_all_feature(self):
        cf = self.get_mg_conn_yellow(dconf.mg_coll_fea)
        init_file = '/home/xuyonglong/feature/working/experiment/201705/sample/test_1029_31'
        # out_file='test'
        # wfp=open(out_file,'w')
        userdic = {}
        with open(init_file, 'r')as fp:
            for line in fp:
                linelist = line.strip().split(',')
                userdic[linelist[0]] = 0

        # cf.update({'_id': phn}, {'$set': {'call': {},'bank':{},'credit_behavior':{}}})
        self.get_bankAndcollection_yellowpage()
        num = 0
        # print userdic.keys()[:5]
        for phn in userdic:
            # phn='13002992136'
            rs = cf.find({'_id': phn})
            if rs.count() > 0:
                if 'call' in rs:
                    if 'max_phn_day_num' in rs['call']:
                        continue
            if not rs or rs.count() == 0:
                # continue
                # print rs
                cf.insert({'_id': phn})
                # continue
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

            # 获取手机系统类型
            # st = self.get_source_type(phn)
            day_no_call,phn_num_list = self.get_maxNocom_sourcetype_lin(phn)
            cf.update({'_id': phn}, {'$set': {'call.bill_variance': phn_num_list[-1],
                                                'call.lately_phn_num': phn_num_list[0],
                                                'call.user_income_all_num': phn_num_list[1],
                                                'call.user_no_ans_num': phn_num_list[2],
                                                'call.contact_num_all_num_ratio': phn_num_list[3],
                                                'call.max_phn_num_day': phn_num_list[4]
                                              }
                                     })
            # daystr=[]
            # for dl in phn_num_list:
            #     daystr.append(str(dl))
            # wfp.write(str(phn)+','+','.join(daystr)+'\n')
            # break
            # print st
            # vip_pass_count, vip_other_vip_count = self.get_contact_feature(phn)
            # print vip_pass_count, vip_other_vip_count
            # pass_count_refuse_count_ratio = -1
            # if refuse_count_all_count==-1:
            #     pass_count_refuse_count_ratio=-1
            # else:
            #     pass_count_refuse_count_ratio=1.0*pass_count_recent_count/(refuse_count_all_count+1)

            # print 'day_no_call',day_no_call
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
            # trade_mony_credit_12, \
            # lottery_sum_12, \
            # consume_abroad_TF, \
            # repay_amount_credit, \
            # repay_count_credit, \
            # consume_amount_3, \
            # consume_amount_6, \
            # consume_count_3, \
            # consume_count_6, \
            # consume_count_airline_12, \
            # withdraw_trade_ratio, \
            # night_trade_amount, \
            # night_trade_count, \
            # transfer_amount_6, \
            # transfer_amount_12, \
            # transfer_count_6, \
            # transfer_count_12, \
            # online_amount_6, \
            # online_count_6, \
            # consume_abroad_count \
            #     = self.consumption_mysql(phn)
            # # print 'trade_mony_credit_12',trade_mony_credit_12
            # cf.update({'_id': phn}, {'$set': {'bank.trade_mony_credit_12': trade_mony_credit_12,
            #                                   'bank.lottery_sum_12': lottery_sum_12,
            #                                   'bank.consume_abroad_TF': consume_abroad_TF,
            #                                   'bank.repay_amount_credit': repay_amount_credit,
            #                                   'bank.repay_count_credit': repay_count_credit,
            #                                   'bank.consume_amount_3': consume_amount_3,
            #                                   'bank.consume_amount_6': consume_amount_6,
            #                                   'bank.consume_count_3': consume_count_3,
            #                                   'bank.consume_count_6': consume_count_6,
            #                                   'bank.consume_count_airline_12': consume_count_airline_12,
            #                                   'bank.withdraw_trade_ratio': withdraw_trade_ratio,
            #                                   'bank.night_trade_amount': night_trade_amount,
            #                                   'bank.night_trade_count': night_trade_count,
            #                                   'bank.transfer_amount_6': transfer_amount_6,
            #                                   'bank.transfer_amount_12': transfer_amount_12,
            #                                   'bank.transfer_count_6': transfer_count_6,
            #                                   'bank.transfer_count_12': transfer_count_12,
            #                                   'bank.online_amount_6': online_amount_6,
            #                                   'bank.online_count_6': online_count_6,
            #                                   'bank.consume_abroad_count': consume_abroad_count,
            #                                   }})
            # application_platforms, \
            # application_pount, \
            # registration_platforms, \
            # registration_count, \
            # overdue_platforms, \
            # overdue_count, \
            # loanlenders_count_6, \
            # loanlenders_count_12, \
            # loanlenders_count, \
            # loanlenders_platforms_6, \
            # loanlenders_platforms_12, \
            # loanlenders_platforms, \
            # rejection_count_6, \
            # rejection_count_12, \
            # rejection_count, \
            # rejection_platforms_6, \
            # rejection_platforms_12, \
            # rejection_platforms \
            #     = self.credit(phn)
            # # print 'applicationPlatforms', applicationPlatforms
            # cf.update({'_id': phn}, {'$set': {'credit_behavior.application_platforms': application_platforms,
            #                                   'credit_behavior.application_count': application_pount,
            #                                   'credit_behavior.registration_platforms': registration_platforms,
            #                                   'credit_behavior.registration_count': registration_count,
            #                                   'credit_behavior.overdue_platforms': overdue_platforms,
            #                                   'credit_behavior.overdue_count': overdue_count,
            #                                   'credit_behavior.loanlenders_count_6': loanlenders_count_6,
            #                                   'credit_behavior.loanlenders_count_12': loanlenders_count_12,
            #                                   'credit_behavior.loanlenders_count': loanlenders_count,
            #                                   'credit_behavior.loanlenders_platforms_6': loanlenders_platforms_6,
            #                                   'credit_behavior.loanlenders_platforms_12': loanlenders_platforms_12,
            #                                   'credit_behavior.loanlenders_platforms': loanlenders_platforms,
            #                                   'credit_behavior.rejection_count_6': rejection_count_6,
            #                                   'credit_behavior.rejection_count_12': rejection_count_12,
            #                                   'credit_behavior.rejection_count': rejection_count,
            #                                   'credit_behavior.rejection_platforms_6': rejection_platforms_6,
            #                                   'credit_behavior.rejection_platforms_12': rejection_platforms_12,
            #                                   'credit_behavior.rejection_platforms': rejection_platforms,
            #                                   # 'credit_behavior.repay_amount_credit': repay_amount_credit,
            #                                   }})
            num += 1
            # break
            if num % 100 == 0:
                time.sleep(1)
                # break

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
