#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
将数据put进mongodb
"""

import os
import sys

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath=os.path.split(os.path.realpath(_abs_path))[0]
sys.path.append(_abs_path)
# sys.path.append(apath+'/log')
import traceback
import json
import pymongo as pm
import time
import datetime
import offline_db_conf as dconf
import MySQLdb
import logging
import shutil
reload(sys)
sys.setdefaultencoding('utf-8')
class PhnData(object):
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
        self.init_mysql_conn()
        self.allphndic={}
        self.relationdic={}
        self.phnxing={}

    def init_mysql_conn(self):
        self.mysqlconn= MySQLdb.connect(host=self.mysqlhost, port=self.mysqlport,
                                        user=self.mysqluser, passwd=self.mysqlpasswd,
                               db=self.mysqldb, charset="utf8")
    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost2], maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self):
        mc = self.mgcollection
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]
    def get_mg_connrelation(self):
        mc = self.mgcollectionrelation
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]

    def put_data_tag(self, mysqlcursor, sql, tag):

        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()

        def p(b):
            re = c.find({'_id': {'$in': b}}, {'somekey': 1})
            rsl = list(re)
            # print len(rsl)
            rerel = crel.find({'_id': {'$in': b}}, {'somekey': 1})
            rslrel = list(rerel)
            # 如果没有somekey 只返回_id, 有的话 返回这个字段和_id;
            # 用来判断该手机号存在与否, 如果不加这个条件会返回整个文档,
            # 增加网络传输时间;
            tu = []  # to return
            tirel = []  # to insert
            turel = []  # to update
            tis = set()
            res = set()
            for i in rsl:
                res.add(i['_id'])
            for i in b:
                if i in res:
                    tu.append(i)
            for i in b:
                if i in rslrel:
                    turel.append(i)
                else:
                    tis.add(i)
            for i in tis:
                tirel.append({'_id': i, tag: True})
            try:
                if turel:
                    crel.update_many({'_id': {'$in': turel}}, {'$set': {tag: True}})
                if tirel:
                    crel.insert_many(tirel)
                    # print len(tu)
                    # if tag == 'is_pass':
                    #     self.put_data_relation(tu, 'contacted_pass_user')
                    # if tag=='is_refuse':
                    #     self.put_data_relation(tu, 'contacted_refuse_user')
            except Exception, e:
                # print str(e)
                pass
            # print len(tu)
            return tu

        try:
            self.mysqlconn.ping()
        except:
            self.init_mysql_conn()
            mysqlcursor = self.mysqlconn.cursor()
        n = mysqlcursor.execute(sql)
        logging.info('The number of %s is :%s', tag, n)
        bphn = []
        if n != 0:
            phncontent = mysqlcursor.fetchall()
            i = 0
            buffer = []
            for fli in phncontent:
                phn = fli[0]
                # print phn
                buffer.append(phn)
                i += 1
                if i % 1000 == 0:
                    bphn += p(buffer)
                    buffer = []
            if i % 1000 != 0:
                bphn += p(buffer)
                # print buffer[1]
        return bphn

    def phn_black(self):
        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        num = 0
        phnlist = []
        for phl in self.allphndic.keys():
            if num < 10000:
                phnlist.append(phl)
                num += 1
            else:
                re = crel.find({'_id': {'$in': phnlist}})
                for rephn in re:
                    if 'is_black' in rephn:
                        self.allphndic[rephn['_id']] = '1'
                    else:
                        self.allphndic[rephn['_id']] = '0'
                phnlist = []
                num = 0

    def fdetail(self, phn):
        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        re = c.find({'_id': phn})
        # print 'phn', phn
        if re.count() < 1:
            rsl = list(re)
            # print rsl
        else:
            self.allphndic[phn] = '-1'
            rsl = list(re)
            if 'call_details' in rsl[0]:
                # print fphn
                if 'call_info' in rsl[0]['call_details']:
                    pcall_info = rsl[0]['call_details']['call_info']
                    for call in pcall_info:
                        if 'phone_num' in call:
                            p = call['phone_num']
                            # print 'p', p
                            p = p.strip()
                            p = p.replace(' ', '').replace('+86', '').replace('-', '').replace(';', '')
                            if len(p) == 13 and p[:2] == '86':
                                p = p[2:]
                            # print p
                            if len(p) > 5:
                                if '*' in p:
                                    print p
                                    self.phnxing[p]=0
                                    self.relationdic.setdefault(phn,{})
                                    self.relationdic[phn].setdefault(p,0)
                                # self.relationdic.setdefault(p, {})
                                # self.relationdic[p].setdefault(phn, 0)
                                # self.allphndic[p] = '-1'

    def put_data_relation(self, fphn,wfp1):
        i = 0
        phndetaillen = 0
        for phn in fphn:
            # print phn
            i += 1
            if i % 100 == 0:
                # print 'relationdic100', len(self.relationdic)
                # print 'allphndic100', len(self.allphndic)
                time.sleep(1)
            self.fdetail(phn)
            if len(self.relationdic)<1:
                continue
            mysqlcursor = self.mysqlconn.cursor()
            sql='select score from risk_fraud_score where mobile ="'+phn+'"'
            c = self.get_mg_conn()
            try:
                self.mysqlconn.ping()
            except:
                self.init_mysql_conn()
                mysqlcursor = self.mysqlconn.cursor()
            n = mysqlcursor.execute(sql)
            score=-1
            if n != 0:
                phncontent = mysqlcursor.fetchall()
                score=phncontent[0][0]
            phnlist=self.relationdic[phn].keys()
            wfp1.write(str(phn)+','+str(score)+','+str(len(phnlist))+','+'|'.join(phnlist)+'\n')
            self.relationdic={}

    def get_data_tag_all(self):
        # daylist = ['2017-04-01', '2017-04-02', '2017-04-03', '2017-04-04', '2017-04-05',
        #            '2017-04-06', '2017-04-07', '2017-04-08', '2017-04-09', '2017-04-10',
        #            '2017-04-11', '2017-04-12', '2017-04-13', '2017-04-14', '2017-04-15', '2017-04-16']

        # daylist=['2017-04-16','2017-04-17','2017-04-18','2017-04-19','2017-04-20',
        #          '2017-04-21','2017-04-22','2017-04-23','2017-04-24','2017-04-25',
        #          '2017-04-26', '2017-04-27', '2017-04-28','2017-04-29','2017-04-30','2017-05-01']
        #
        # daylist = ['2017-05-01', '2017-05-02', '2017-05-03', '2017-05-04', '2017-05-05',
        #            '2017-05-06', '2017-05-07', '2017-05-08', '2017-05-09', '2017-05-10',
        #            '2017-05-11', '2017-05-12', '2017-05-13', '2017-05-14', '2017-05-15', '2017-05-16']
        #
        # daylist=['2017-05-16','2017-05-17','2017-05-18','2017-05-19','2017-05-20',
        #          '2017-05-21','2017-05-22','2017-05-23','2017-05-24','2017-05-25',
        #          '2017-05-26', '2017-05-27', '2017-05-28','2017-05-29','2017-05-30','2017-05-31','2017-06-01']
        # daylist = ['2017-05-10',
        #            '2017-05-11', '2017-05-12', '2017-05-13', '2017-05-14', '2017-05-15', '2017-05-16']

        daylist = ['2017-09-26', '2017-09-27']
        for i in range(0, len(daylist) - 1):
            leftday = daylist[i]
            rightday = daylist[i + 1]
            if os.path.exists("graphxdata/" + leftday):
                shutil.rmtree("graphxdata/" + leftday)
            # os.remove("graphxdata/"+leftday)
            os.mkdir("graphxdata/" + leftday)
            out_file1 = "graphxdata/" + leftday + '/vertex'
            out_file2 = "graphxdata/" + leftday + '/relation'
            wfp1 = open(out_file1, 'w')
            wfp2 = open(out_file2, 'w')
            mysqlcursor = self.mysqlconn.cursor()
            sql = 'select DISTINCT(uu.mobile) from risk_credit_chain rcc ' \
                  'join risk_user_merchant rum on rum.userid=rcc.userid ' \
                  'join uc_users uu on uu.id=rum.merchantUserid ' \
                  'where rcc.createDate>="' + leftday + '" and rcc.createDate<"' + rightday + '"'
            c = self.get_mg_conn()
            try:
                self.mysqlconn.ping()
            except:
                self.init_mysql_conn()
                mysqlcursor = self.mysqlconn.cursor()
            n = mysqlcursor.execute(sql)
            if n != 0:
                phncontent = mysqlcursor.fetchall()
                i = 0
                buffer = []
                for fli in phncontent:
                    phn = fli[0]
                    # print phn
                    buffer.append(phn)
                    i += 1
                    if i % 1000 == 0:
                        self.put_data_relation(buffer,wfp2)
                        buffer = []
                        # break
                if i % 1000 != 0:
                    self.put_data_relation(buffer,wfp2)
            # print "*",len(self.phnxing)
            # print 'all',len(self.allphndic)
            # print 'scale',1.0*len(self.phnxing)/len(self.allphndic)
            # self.allphndic = {}
            # for phd in self.relationdic:
            #     rightdic = self.relationdic[phd]
            #     for rp in rightdic:
            #         try:
            #             wfp2.write(str(phd) + ',' + str(rp) + '\n')
            #         except:
            #             print phd, rp
            #             continue
            # self.relationdic = {}

if __name__ == '__main__':
    p = PhnData()
    p.get_data_tag_all()