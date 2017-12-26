#! /usr/bin/env python
# -*- encoding: utf-8 -*-

import sys
import os
# import web
import math
from bin.api.model.risk.user_info import CardId, CreditTask
# from http_exp import HttpJsonError
# from tools.aam import *
import config.common_conf as conf
import config.db_conf as dconf
import datetime
import json
import pymongo as pm
from bin.api.controller.common.jklogger import JKLOG
import threading
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class AntiFraud(object):
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
        self.init_mg_conn()
        self.conn_old = None
        self.init_mg_conn_old()
        # mysql
        self.db_engine = create_engine(dconf.DB_USER_BASIC_INFO, encoding='utf-8',pool_recycle=30)
        self.Session = sessionmaker(bind=self.db_engine)
        # fea
        self.fea_schm_file = dconf.fea_schm_file
        self.feature_schema = {}
        self.load_fea_schm()
        self.feature = []
        #print '11111', self.feature_schema
        # model
        self.jk_app_model_dir = dconf.risk_jk_app_model
        self.jk_h5_model_dir = dconf.risk_jk_h5_model
        self.jk_app_model = {}
        self.jk_h5_model = {}
        self.jk_app_bins = {}
        self.jk_h5_bins = {}
        self.load_model_all()
        #print '22222', self.jk_app_model
        #print '33333', self.jk_h5_model
        #print '44444', self.jk_app_bins
        #print  '5555', self.jk_h5_bins
        # get dict data
        self.aam = UAAMachine()
        self.load_dict_contacts_name()
        self.aam.build_automation()

    def init_mg_conn(self):
        self.conn = pm.MongoClient([self.mhost1, self.mhost2],replicaSet=self.mreplicat_set, maxPoolSize = 10)
        self.conn[dconf.mg_db].authenticate(self.muname, self.mpasswd)
        try:
            self.conn.server_info()
        except Exception, e:
            JKLOG.Error([('e', 'mongodb error'), ('exp type', str(e))])
            raise HttpJsonError(610, 'DB Error')

    def init_mg_conn_old(self):
        self.conn_old = pm.MongoClient([dconf.o_mg_host1, dconf.o_mg_host2], replicaSet=dconf.o_mg_replicat_set, maxPoolSize = 10)
        self.conn_old[dconf.o_mg_db].authenticate(dconf.o_mg_uname, dconf.o_mg_passwd)
        try:
            self.conn_old.server_info()
        except Exception, e:
            JKLOG.Error([('e', 'mongodb error'), ('exp type', str(e))])
            raise HttpJsonError(610, 'DB Error')
            
    def get_mg_conn(self, mc):
        mdb = dconf.mg_db
        #mc = dconf.mg_collection
        #if mdb not in self.conn.database_names():
        #    JKLOG.Error([('e', 'mongodb error'), ('exp type', 'no this db %s'%mdb)])
        #    raise HttpJsonError(610, 'DB Error')
        mdb = self.conn[dconf.mg_db]
        #if mc not in mdb.collection_names():
        #    JKLOG.Error([('e', 'mongodb error'), ('exp type', 'no this collection %s'%mc)])
        #    raise HttpJsonError(610, 'DB Error')
        return mdb[mc]

    def get_mg_conn_old(self, mc):
        mdb = dconf.o_mg_db
        mdb = self.conn_old[dconf.o_mg_db]
        return mdb[mc]
        
    def is_phn_exist(self, phn):
        c = self.get_mg_conn(dconf.mg_collection)
        if not c.find_one({'_id':phn}):
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
        m = {} # model
        f = {} # feature 
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
            if (v not in self.jk_app_bins) or (v not in self.jk_app_model):
                raise HttpJsonError(700, 'no this version %s' % v)
            self.calc_feature_jk_app(phn, v)
            s = self.calc_score(self.jk_app_model[v])
        elif app_type == 1:
            if (v not in self.jk_h5_bins) or (v not in self.jk_h5_model):
                raise HttpJsonError(700, 'no this version %s' % v)
            self.calc_feature_jk_h5(phn, v)
            s = self.calc_score(self.jk_h5_model[v])
        else:
            raise HttpJsonError(601, 'app type dose not exist')
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
            s += 1.0*self.feature[i]*w[i]
        s = 1.0/(1+math.exp(-s))
        if l == 1:
            s = 1-s
        return s
        
    def calc_feature_jk_app(self, phn, v):
        self.feature = []
        f1 = self.get_jk_basic_fea(phn)
        self.feature += f1
        f2 = self.get_comm_fea(phn)
        self.feature += f2
        #self.store_feature(phn)
        self.split_feature_ohe(phn, self.jk_app_bins[v])
        
    def calc_feature_jk_h5(self, phn, v):
        self.feature = []
        f1 = self.get_jk_basic_fea(phn)
        self.feature += f1
        f2 = self.get_comm_fea(phn)
        self.feature += f2
        #self.store_feature(phn)
        self.split_feature_ohe(phn, self.jk_h5_bins[v])

    def get_diantance_yellowpage_fea(self):
        aaa=1

    def get_jk_basic_fea(self, phn):
        #phn = '13810288567'
        # 获取基本特征,比如性别／年龄等不需要计算的特征
        session = self.Session()
        try:
            re = session.query(CardId.sex, CardId.age).join( 
                CreditTask, CardId.userId == CreditTask.userId).filter(
                CreditTask.userName == phn).first()
        except Exception, e:
            JKLOG.Error([('e', 'get basic info failue'), ('exp type', str(e))])
            raise HttpJsonError(602, 'Get Info From DB Error')
        #print re
        if re is None:
            JKLOG.Info([('e','get basic info failue, no info'), ('phone', phn)])
            return [0, 10] # 默认为男 10岁
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
        #print gender, age
        session.close()
        return [gender, age]
        
    def get_comm_fea(self, phn):
        # 获取通讯录／通讯详单的特征
        contacts = []
        call_details = {}
        c = self.get_mg_conn(dconf.mg_collection)
        cinfo = c.find_one({'_id': phn})
        if 'contacts' in cinfo:
            contacts = cinfo['contacts']
        else:
            JKLOG.Info([('get contacts', phn), ('error', 'no contacts')])
        if 'call_details' in cinfo:
            call_details = cinfo['call_details']
        else:
            JKLOG.Info([('get call_details', phn), ('error', 'no call_details')])
        if not contacts and not call_details:
            raise HttpJsonError(603, 'Get Info From MgDB Error')
        return self.get_comm_contacts_call_details_fea(phn, contacts, call_details)
        
    def get_comm_contacts_call_details_fea(self, phn, contacts, call_details):
        f = []
        if 'call_info' not in call_details:
            JKLOG.Info([('data', 'call_details'), ('phone', phn), ('info', 'no call_info')])
            return []
        try:
            f.append(self.fea_open_time(call_details['innet_date']))
        except Exception, e:
            JKLOG.Info([('phone', phn), ('call_details error', str(e))])
            f.append(1) # 如果出现异常 设为1个月
        name_stat, sens, family, job, srv, phn_sum, phns = self.pro_contacts(contacts)
        f.append(phn_sum)
        cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        anchor1 = self.before_some_days(cur_time, 30)
        anchor2 = self.before_some_days(cur_time, 60)
        try:
            max_comm_plac = call_details['max_comm_plac']
            first_call_time = call_details['call_info'][0]['start_time']
            anchor1 = self.before_some_days(first_call_time, 30)
            anchor2 = self.before_some_days(first_call_time, 60)
        except Exception, e:
            JKLOG.Error([('data', 'call_details'), ('phone', phn), ('error', str(e))])
            #raise HttpJsonError(700, 'Data Error')
        call_stat = {} 
        # phn: [locate_call_num, remote_call_num, call_time_length]
        # 一个手机号的locate_call_num 和 remote_call_num 一定只有一个> 0
        time_seg = [[0, 0, 0], [0, 0, 0]] # [new:早 中 晚, old:早 中 晚]
        time_len = [[0, 0], [0, 0]] # [new:通话长度和 通话手机号和, old: 通话长度和 通话手机号和]
        time_frq = [{}, {}] # 通话频率
        name_cate = [0, 0, 0, 0] # 
        cantact_person = [set(), set()] # 两个月的通话手机号
        for c in call_details['call_info']:
            try:
                phn = c['phone_num']
                call_stat.setdefault(phn, [0, 0, 0])
                cl = int(round(float(c['comm_time'])/60))
                call_stat[phn][2] += cl
                if c['comm_type'].find(u'本地') or c['comm_plac'] == max_comm_plac:
                    # 本地
                    call_stat[phn][0] += 1
                else:
                    call_stat[phn][1] += 1
                st = c['start_time']
                d = st.split(' ')[0]
                ts = self.get_time_seg(st)
                if st > anchor1: # 最近一个月
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
        time_frq[0] = time_frq[0].values() + [0] # 避免为空集
        time_frq[1] = time_frq[1].values() + [0]
        live_contact_person_count = 0
        name_call_fea = [0, 0, 0, 0, 0, 0, 0, 0, 0]
        locate_remote_count = [0, 0, 0, 0] 
        # locate_person_count remote_person_count locate_call_sum remote_call_sum
        for k, v in call_stat.items():
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
                    name_call_fea[0] += 1 # name_frq_call_count
                    name_call_fea[1] += vs # name_frq_call_num
                    name_call_fea[2] += v[2] # name_frq_call_time
                else:
                    name_call_fea[3] += 1 # name_call_count
                    name_call_fea[4] += vs # name_call_num
                    name_call_fea[5] += v[2] # name_call_time
            else:
                name_call_fea[6] += 1 # no_name_call_count
                name_call_fea[7] += vs # no_name_call_num
                name_call_fea[8] += v[2] # no_name_call_time
        rltn_fea = self.get_rltn_fea(call_stat.keys())
        f += [
            1.0*locate_remote_count[0]/(locate_remote_count[1]+1),
            1.0*locate_remote_count[2]/(locate_remote_count[3]+1),
            sum(time_seg[0]) - sum(time_seg[1]),
            time_seg[0][0] - time_seg[0][1],
            time_seg[0][1] - time_seg[0][2],
            time_seg[1][0] - time_seg[1][1],
            time_seg[1][1] - time_seg[1][2],
            (1.0*time_len[0][0]/(time_len[0][1]+1) -
             1.0*time_len[1][0]/(time_len[1][1]+1)),
            max(time_frq[0]) - max(time_frq[1]),
            sum(time_frq[0])/4.0 - sum(time_frq[1])/4.0,
            len(cantact_person[0] - cantact_person[1]),
            1.0*live_contact_person_count/(phn_sum+1),
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
        rltn_one = [0, 0, 0] # balck pass refuse 1度关系
        rltn_two = [0, 0, 0, 0] # pass pass_contact refuse refuse_contact 2度关系

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
        c = self.get_mg_conn_old(dconf.o_mg_collection)
        for i in range(0, len(phns), 50):
            phns_part = phns[i:i+50]
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
        return rltn_one + rltn_two
        
    def fea_open_time(self, open_time):
        open_time = datetime.datetime.strptime(open_time, "%Y-%m-%d %H:%M:%S")
        cur_time = datetime.datetime.now()
        open_time_length = int(round(1.0*(cur_time - open_time).days/30))
        return open_time_length
    
    def before_some_days(self, d, delta):
        return (datetime.datetime.strptime(d,"%Y-%m-%d %H:%M:%S") -
                datetime.timedelta(days=delta)).strftime("%Y-%m-%d %H:%M:%S")
        
    def get_time_seg(self, d):
        h = datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S").hour
        ts  = 0
        if h >= 7 and h <= 12:
            ts = 0
        elif h > 12 and h <= 18:
            ts = 1
        else:
            ts = 2
        return ts
        
    def pro_contacts(self, contacts):
        name_len = {} # len : count
        phns = []
        sens = [] # cate tag : 0
        family = [] # cate tag : 1
        job = [] # cate tag : 2
        srv = [] # cate tag : 3
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
            #print n
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
        name_len = sorted(name_len.items(), key = lambda x:x[0])
        tar_len_index = 1
        if len(name_len) > 3: 
            tar_len_index = 3
        try:
            name_stat_1 = reduce(lambda x,y:x[1] + y[1], name_len[:tar_len_index])
            name_stat_2 = reduce(lambda x,y:x[1] + y[1], name_len)
            name_stat = 1.0*name_stat_1/name_stat_2
        except Exception, e:
            pass
        return (name_stat, sens, family, job, srv, len(phns), phns)
        
    def split_feature_ohe(self, sid, bins):
        fea = self.feature[:]
        JKLOG.Info([('x', 'feature'), ('sample_id', sid), ('origin feature', fea)])
        #print self.feature
        for i in range(len(self.feature)):
            nv = 0
            cfv = self.feature[i]
            one_hot = [0]*(len(bins[str(i)])+1)
            for j in bins[str(i)]:
                if cfv <= j:
                    one_hot[nv] = 1
                    break
                nv += 1
            self.feature[i] = one_hot
            fea[i] = nv
        #print self.feature
        self.feature = reduce(lambda x,y:x+y, self.feature)
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
            
    def __del__(self):
        self.conn.close()
