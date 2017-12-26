#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
获取用户所有通话记录，然后生成顶点文件和关系文件
同一天来认证的用户生成一个顶点文件和一个关系文件，并存放到以当天日期为文件夹名的文件夹里
顶点文件格式：(13616274217,0);代表的意思为：(编号(从0一直往后累加，不重复)，电话号，是否在黑名单里（0：不是，1:是）)
关系文件格式：(13595222727,13595235888);代表的意思为：(电话号1，电话号2)，说明电话号1和电话号2有联系
"""

import os
import sys

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath=os.path.split(os.path.realpath(_abs_path))[0]
sys.path.append(_abs_path)
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
        self.daylist=dconf.daylist

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
                phnlist=[]
                num=0

    def fdetail(self,phn):
        c = self.get_mg_conn()
        re = c.find({'_id': phn})
        if re.count()>=1:
            self.allphndic[phn] = '-1'
            rsl=list(re)
            if 'call_details' in rsl[0]:
                if 'call_info' in rsl[0]['call_details']:
                    pcall_info = rsl[0]['call_details']['call_info']
                    for call in pcall_info:
                        if 'phone_num' in call:
                            p = call['phone_num']
                            p = p.strip()
                            p = p.replace(' ', '').replace('+86', '').replace('-', ''). \
                                replace('*86', '').replace(';', '')
                            if len(p) == 13 and p[:2] == '86':
                                p = p[2:]
                            if len(p) > 5:
                                self.relationdic.setdefault(p,{})
                                self.relationdic[p].setdefault(phn, 0)
                                self.allphndic[p] = '-1'

    def put_data_relation(self, fphn):
        i = 0
        for phn in fphn:
            i += 1
            if i % 100 == 0:
                time.sleep(1)
            self.fdetail(phn)

    def get_data_tag_all(self):
        for i in range(0,len(self.daylist)-1):
            leftday=self.daylist[i]
            rightday=self.daylist[i+1]
            if os.path.exists('/home/hadoop/xuyonglong/graphxdata/'+leftday) :
                shutil.rmtree('/home/hadoop/xuyonglong/graphxdata/'+leftday)
            os.mkdir('/home/hadoop/xuyonglong/graphxdata/'+leftday)
            vertexfile='/home/hadoop/xuyonglong/graphxdata/'+leftday+'/vertex'
            relationfile='/home/hadoop/xuyonglong/graphxdata/'+leftday+'/relation'
            wfp1=open(vertexfile,'w')
            wfp2=open(relationfile,'w')
            mysqlcursor = self.mysqlconn.cursor()
            sql = 'select DISTINCT(uu.mobile) from risk_credit_chain rcc ' \
                  'join risk_user_merchant rum on rum.userid=rcc.userid ' \
                  'join uc_users uu on uu.id=rum.merchantUserid ' \
                  'where rcc.createDate>="'+leftday+'" and rcc.createDate<"'+rightday+'"'
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
                    buffer.append(phn)
                    i += 1
                    if i % 1000 == 0:
                        self.put_data_relation(buffer)
                        buffer = []
                if i % 1000 != 0:
                    self.put_data_relation(buffer)
            self.phn_black()
            for phd in self.allphndic:
                try:
                    wfp1.write(str(phd)+','+self.allphndic[phd]+'\n')
                except:
                    print phd,self.allphndic[phd]
                    continue
            self.allphndic={}
            for phd in self.relationdic:
                rightdic= self.relationdic[phd]
                for rp in rightdic:
                    try:
                        wfp2.write(str(phd)+','+str(rp)+'\n')
                    except:
                        print phd,rp
                        continue
            self.relationdic={}
    def get_phn_data(self):
        aa=1
if __name__ == '__main__':
    p=PhnData()
    # p.get_data_tag_all()

