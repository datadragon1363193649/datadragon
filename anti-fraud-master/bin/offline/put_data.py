#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
将数据put进mongodb

author: xiaoliang.liu at jiandanjiekuan dot com
date: 2017-02-28
"""

import os
import sys
_abs_path = os.path.split(os.path.realpath(__file__))[0]
sys.path.append(_abs_path)
_lib_path = _abs_path + '/../../lib'
_conf_path = _abs_path + '/../../config'
sys.path.append(_conf_path)
sys.path.append(_lib_path + '/utils')
sys.path.append(_lib_path + '/common')
import json
import pymongo as pm
import datetime

import common_conf as conf
import db_conf as dconf
from jklogger import *
from basic import ZnCharProcess

_debug = False

class PutData(object):
    def __init__(self):
        self.mhost = dconf.mg_host
        self.mport = dconf.mg_port
        self.mdb = dconf.mg_db
        self.mc = dconf.mg_collection
        self.conn = pm.MongoClient(host = self.mhost, port = self.mport)
        try:
            self.conn.server_info()
        except:
            # log
            exit(1)
        if self.mdb not in self.conn.database_names():
            # log
            exit(1)
        self.db = self.conn[self.mdb]
        if self.mc not in self.db.collection_names():
            # log
            exit(1)
        self.c = self.db[self.mc]
        
        self.cp = ZnCharProcess()
        
    def put_contacts(self):
        cdir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test1/contacts'
        flist = os.listdir(cdir)
        uid_phn_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test1/sample/uid_phn'
        uid_phn = {}
        with open(uid_phn_file, 'r') as fp:
            for line in fp:
                uid, phn = line.strip().split('\t')
                uid_phn[uid] = phn
        if _debug:
            uid_phn = {'55796': '18445866434'}
        for f in flist:
            if f not in uid_phn: continue
            phn = uid_phn[f]
            if self.c.find({'_id': phn}).count() == 0:
                self.c.insert({'_id': phn})
            cstr = ''
            with open(cdir + '/' + f, 'r') as fp:
                cstr = fp.readline()
            cstr = json.loads(cstr)
            new_cstr = []
            for item in cstr:
                name = item['name']
                if not name.strip(): continue
                phone = item['phone']
                tokens = []
                for t in name:
                    if not self.cp.isOther(t):
                        tokens.append(t)
                name = ''.join(tokens)
                new_phn = []
                for p in phone:
                    if p:
                        p = p.replace(' ', '').replace('+86', '').replace('-', '')
                        if len(p) == 13 and p[:2] == '86': p = p[2:]
                        new_phn.append(p)
                new_cstr.append({'name': name, 'phone': new_phn})
            if new_cstr:
                self.c.update({'_id': phn},  {"$set":{"contacts": new_cstr}})

    def put_details(self):
        ddir = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test1/details'
        flist = os.listdir(ddir)
        uid_phn_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test1/sample/uid_phn'
        uid_phn = {}
        with open(uid_phn_file, 'r') as fp:
            for line in fp:
                uid, phn = line.strip().split('\t')
                uid_phn[uid] = phn
        if _debug:
            uid_phn = {'55796': '18445866434'}
        for f in flist:
            if f not in uid_phn: continue
            phn = uid_phn[f]
            if self.c.find({'_id': phn}).count() == 0:
                self.c.insert({'_id': phn})
            dstr = ''
            with open(ddir + '/' + f, 'r') as fp:
                dstr = fp.readline()
            dstr = json.loads(dstr)
            dstr = self.pro_details(dstr)
            if dstr:
                self.c.update({'_id': phn},  {"$set":{"call_details": dstr}})
    
    def reverse_contacts(self):
        phn_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test1/sample/uid_phn'
        with open(phn_file, 'r') as fp:
            for line in fp:
                p = line.strip().split('\t')[1]
                contacts = self.c.find_one({'_id': p})['contacts']
                for c in contacts:
                    plist = c['phone']
                    for pi in plist:
                        if self.c.find({'_id': pi}).count() == 0:
                            self.c.insert({'_id': pi})
                        self.c.update({'_id': pi}, {"$addToSet":{'contacted_user':p}})

    def put_relations(self):
        black_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/black'
        pass_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/pass'
        refuse_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/refuse'
        pass_contact_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/pass_contacts'
        refuse_contact_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/refuse_contacts'
        uid_phn_file = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/raw/phonenum_pool/uid_phn'
        uid_phn = {}
        with open(uid_phn_file, 'r') as fp:
            for line in fp:
                uid, phn = line.strip().split('\t')
                uid_phn[uid] = phn
        def f1(f, tag):
            with open(f, 'r') as fp:
                for line in fp:
                    p = ''
                    if len(line.strip().split('\t')) == 2:
                        p = line.strip().split('\t')[1] # refuse pass
                    else:
                        p = line.strip() # black
                    if self.c.find({'_id': p}).count() == 0:
                        self.c.insert({'_id': p})
                    self.c.update({'_id': p},  {"$set":{tag: True}})
        def f2(f, tag):
            with open(f, 'r') as fp:
                for line in fp:
                    linelist = line.strip().split('\t')
                    u = linelist[0]
                    clist = linelist[1:]
                    if u not in uid_phn: continue
                    phn = uid_phn[u]
                    for c in clist:
                        if self.c.find({'_id': c}).count() == 0:
                            self.c.insert({'_id': c})
                        self.c.update({'_id': c}, {"$addToSet":{tag:phn}})
        f1(black_file, 'is_black')
        f1(pass_file, 'is_pass')
        f1(refuse_file, 'is_refuse')
        
        f2(pass_contact_file, 'contacted_pass_user')
        f2(refuse_contact_file, 'contacted_refuse_user')
        
    def pro_details(self, dstr):
        if 'code' in dstr:
            return self.pro_details_wc(dstr)
        else:
            return self.pro_details_lhp(dstr)

    def pro_details_lhp(self, dstr):
        open_time = dstr['phoneList'][0]['registerDate']
        call_info = dstr['phoneList'][0]['telData']
        if open_time == '':
            open_time = 'Wed Sep 24 21:56:38 CST 2016'
        open_time = datetime.datetime.strptime(open_time, "%a %b %d %H:%M:%S %Z %Y")
        open_time = datetime.datetime.strftime(open_time, "%Y-%m-%d %H:%M:%S")
        call_list = []
        city = {}
        for c in call_info:
            try:
                phn = c['receiverPhone']
                st = c['cTime']
                if st == '':
                    st = '2016-11-27 9:40:03'
                else:
                    st = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(st)/1000))
                cl = c['tradeTime']
                call_type = c['callType'] # 主叫 被叫
                comm_plac = c['tradeAddr']
                city.setdefault(comm_plac, 0)
                city[comm_plac] += 1
                comm_type = u"本地" # 本地 异地
                if 'tradeType' in c:
                    comm_type = c['tradeType']
                call_list.append({
                    'phone_num': phn,
                    'start_time': st,
                    'comm_time': cl,
                    'comm_mode': call_type,
                    'comm_plac': comm_plac,
                    'comm_type': comm_type})
            except Exception, e:
                # log
                continue
        max_city = max(city.items(), key = lambda x:x[1])[0]
        return {
            'innet_date': open_time,
            'call_info': call_list,
            'max_comm_plac': max_city}
        
    def pro_details_wc(self, dstr):
        open_time = ''
        if 'innetDate' in dstr['data']['transportation'][0]['origin']['baseInfo']['data']:
            open_time = dstr['data']['transportation'][0]['origin']['baseInfo']['data']['innetDate']
        call_info = dstr['data']['transportation'][0]['origin']['callInfo']['data']
        if open_time == '':
            open_time = '20160101'
        try:
            open_time = datetime.datetime.strptime(open_time, "%Y%m%d")
            open_time = datetime.datetime.strftime(open_time, "%Y-%m-%d %H:%M:%S")
        except Exception, e:
            pass
        call_list = []
        city = {}
        for c in call_info:
            if c['code'] != 'E000000': continue
            if 'details' not in c:
                continue
            for cd in c['details']:
                try:
                    phn = cd['anotherNm']
                    st = cd['startTime']
                    if st == '':
                        st = '2016-11-27 9:40:03'
                    elif st.find('-') == 2:
                        st = '2016-' + st
                    elif st.find('-') > 0 and st.find('/') > 0:
                        st = st.split('-')[1]
                    elif len(st.split('-')) == 4:
                        st = '-'.join(st.split('-')[1:])
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
                    cl = h*3600 + m*50 + s
                    ca = u"本地"
                    if 'commType' in cd:
                        ca = cd['commType'] # 本地 其他
                    if ca.find("本地".decode('utf-8')) >=0 or ca.find("市话".decode('utf-8')) >= 0 or ca.find("国内".decode('utf-8')) >= 0:
                        ca = u"本地"
                    else:
                        ca = u"异地"
                    comm_mode = cd['commMode']
                    comm_plac = cd['commPlac']
                    city.setdefault(comm_plac, 0)
                    city[comm_plac] += 1
                    call_list.append({
                        'phone_num': phn,
                        'start_time': st,
                        'comm_time': cl,
                        'comm_mode': comm_mode,
                        'comm_plac': comm_plac,
                        'comm_type': ca})
                except Exception, e:
                    # log
                    continue
        if not city: 
            max_city = ""
        else:
            max_city = max(city.items(), key = lambda x:x[1])[0]
        return {
            'innet_date': open_time,
            'call_info': call_list,
            'max_comm_plac': max_city}
        
if __name__ == '__main__':
    pd = PutData()
    pd.reverse_contacts()
