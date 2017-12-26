#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
获取某些指定用户所有通话记录，并生成编号文件
用一个用户的所有通话过的电话号码与训练数据顶点文件匹配。匹配到的编号存到以该用户电话为文件名的文件里
prephn文件是指定用户电话的集合
编号文件是指某用户

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



class PrePhnData(object):
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
        self.verdic={}
        self.phnlist=dconf.prephnlist

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
                            if len(p) == 11:
                                if p in self.verdic:
                                    self.allphndic[self.verdic[p]] = '-1'

    def put_data_relation(self, fphn):
        self.fdetail(fphn)
    def get_data_tag_all(self):
        prephnfile = '/home/hadoop/xuyonglong/graphxdata/prevertex_11/prephn'
        vertexidfile='/home/hadoop/xuyonglong/graphxdata/vertex_id_16_18'
        with open(vertexidfile,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                self.verdic[linelist[1]]=linelist[0]
        print len(self.verdic)
        wfp2 = open(prephnfile, 'w')
        for i in range(0,len(self.phnlist)):
            phn=self.phnlist[i]
            phnfile='/home/hadoop/xuyonglong/graphxdata/prevertex_11/'+phn
            wfp1=open(phnfile,'w')
            self.fdetail(phn)
            for phd in self.allphndic:
                try:
                    wfp1.write(str(phd)+'\n')
                except:
                    print phd,self.allphndic[phd]
                    continue
            self.allphndic={}
            wfp2.write(phn+'\n')
if __name__ == '__main__':
    p=PrePhnData()
    p.get_data_tag_all()

