#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
将数据put进mongodb

author: xiaoliang.liu at jiandanjiekuan dot com
date: 2017-02-28
"""

import os
import sys
import xlrd

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath=os.path.split(os.path.realpath(_abs_path))[0]
sys.path.append(apath)
# sys.path.append(apath+'/log')
import traceback
import json
import pymongo as pm
import time
import datetime
import offline_db_conf as dconf
import pandas as pda
from numpy import *
from math import*
# import config.offline_db_conf as dconf
# import MySQLdb
import logging
# logging.basicConfig(level=logging.DEBUG,
#                 format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                 datefmt='%a, %d %b %Y %H:%M:%S',
#                 filename=apath+'/log/'+dconf.log_filename,
#                 filemode='a')
_debug = False


class PutData(object):
    def __init__(self):
        self.mhost1 = dconf.mg_host1
        self.mhost2 = dconf.mg_host2
        self.mreplicat_set = dconf.mg_replicat_set
        self.mgdb=dconf.mg_db
        self.mgcollection=dconf.mg_collection
        self.mgcollectionrelation = dconf.mg_coll_relation
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.mgconn = None
        self.init_mg_conn()
        self.mysqlhost=dconf.mysql_host
        self.mysqlport=dconf.mysql_port
        self.mysqluser=dconf.mysql_user
        self.mysqlpasswd=dconf.mysql_passwd
        self.mysqldb=dconf.mysql_db
        self.mysqlconn=None
        # self.init_mysql_conn()

    # def init_mysql_conn(self):
    #     self.mysqlconn= MySQLdb.connect(host=self.mysqlhost, port=self.mysqlport, user=self.mysqluser, passwd=self.mysqlpasswd,
    #                            db=self.mysqldb, charset="utf8")
    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost1, self.mhost2], replicaSet=self.mreplicat_set, maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self):
        mc = self.mgcollection
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]
    def get_mg_connrelation(self):
        mc = self.mgcollectionrelation
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]

    def put_data_tag(self, phonelist,namelist, tag):

        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def p(bufdic):
            phone=bufdic.keys()
            re = c.find({'_id': {'$in': phone}}, {'somekey': 1})
            rsl = list(re)
            rerel = crel.find({'_id': {'$in': phone}}, {'somekey': 1})
            rslrel = list(rerel)
            tu = []  # to return
            tirel = []  # to insert
            turel = []  # to update
            namelist=[]
            tis = set()
            res = set()
            for i in rsl:
                res.add(i['_id'])
            for i in phone:
                if i in res:
                    tu.append(i)
            for i in phone:
                if i in rslrel:
                    turel.append(i)
                    namelist.append(bufdic[i])
                else:
                    tis.add(i)
            for i in tis:
                tirel.append({'_id': i, tag: True,'yellowname':bufdic[i]})
            try:
                if turel:
                    crel.update_many({'_id': {'$in': turel}}, {'$set': {tag: True,'yellowname':namelist}})
                if tirel:
                    crel.insert_many(tirel)
            except Exception, e:
                # print str(e)
                pass
            # print len(tu)
            return tu
        i = 0
        bufferpho = {}
        buname=[]
        for i in range(0,len(phonelist)):
            # print phn
            bufferpho[phonelist[i]]=namelist[i]
            i += 1
            if i % 1000 == 0:
                p(bufferpho)
                bufferpho = {}
        if i % 1000 != 0:
            p(bufferpho)
        # print buffer[1]

    def put_data_tag_all(self):

        bankyellowdf = pda.read_excel('/Users/ufenqi/Desktop/bank_yellowpage.xlsx')
        cuishouyellowdf = pda.read_excel('/Users/ufenqi/Desktop/collection_yellowpage.xlsx')
        bankyellowdf.columns = ['source', 'phone']
        cuishouyellowdf.columns = ['source', 'phone']
        bankmobile = bankyellowdf['phone'].values.tolist()
        cuishoumobile = cuishouyellowdf['phone'].values.tolist()
        bankname = bankyellowdf['source'].values.tolist()
        cuishouname = cuishouyellowdf['source'].values.tolist()
        self.put_data_tag(bankmobile,bankname, 'is_bank_yellowpage')
        self.put_data_tag(cuishoumobile,cuishouname, 'is_collection_yellowpage')

    def put_data_relation(self, fphn, tag):
        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def pdetail(b, t):
            re = crel.find({'_id': {'$in': b}}, {'somekey': 1})
            ti = []  # to insert
            tu = []  # to update
            res = set()
            for i in re:
                res.add(i['_id'])
            b = set(b)
            if t in b:
                b.remove(t)  # 从联系人中去除自己
            for i in b:
                if i in res:
                    tu.append(i)
                else:
                    ti.append({'_id': i, tag: [t], 'contacted_user': [t]})
            try:
                if tu:
                    crel.update_many({'_id': {'$in': tu}}, {'$addToSet': {tag: t, 'contacted_user': t}})
                if ti:
                    crel.insert_many(ti)
            except Exception, e:
                # print str(e)
                # traceback.print_exc()
                # print t
                pass
        def fdetail(phn):
            re = c.find({'_id':phn})
            rsl = list(re)
            # print re[0]['call_details'][0]
            phndetail=[]
            if 'call_details' in rsl[0]:
                # print fphn
                if 'call_info' in rsl[0]['call_details']:
                    # print rsl[0]['call_details'][0]
                    pcall_info=rsl[0]['call_details']['call_info']
                    for call in pcall_info:
                        if 'phone_num' in call:
                            p=call['phone_num']
                            p = p.strip()
                            p = p.replace(' ', '').replace('+86', '').replace('-', '').replace('*86', '').replace(';', '')
                            if len(p) == 13 and p[:2] == '86':
                                p = p[2:]
                            # print p
                            if len(p)>5:
                                phndetail.append(p.encode('utf-8'))
                                # print 'dayu',p
                    # print len(phndetail)
                    pdetail(phndetail,phn)
            return len(phndetail)
        i = 0
        buffer = {}
        phndetaillen=0
        for phn in fphn:
            i += 1
            if i % 100 == 0:
                # print "Start : %s" % time.ctime()
                time.sleep(1)
                # print "End : %s" % time.ctime()
            # print phn
            phndetaillen+=fdetail(phn)
        # print 'detail done'
        return phndetaillen
    # def put_data_relation_all(self):
    #     pass_contacted_fp = _abs_path + '/data/pass_contacts'
    #     refuse_contacted_fp = _abs_path + '/data/refuse_contacts'
    #     self.put_data_relation(pass_contacted_fp, 'contacted_pass_user')
    #     self.put_data_relation(refuse_contacted_fp, 'contacted_refuse_user')


if __name__ == '__main__':
    pd = PutData()
    pd.put_data_tag_all()
    # print apath
    # print dconf
    # pd.put_data_relation_all()
    # phn=['13871325741']
    # pd.put_data_relation(phn,'contacted_pass_user')