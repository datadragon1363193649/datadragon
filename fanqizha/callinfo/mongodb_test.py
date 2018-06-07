#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
将数据put进mongodb

author: xuyonglong
"""

import os
import sys
# import pandas as pda
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
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
import logging
import copy


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

    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost1, self.mhost2],
                                     replicaSet=self.mreplicat_set, maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self):
        mc = self.mgcollection
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]

    def get_mg_connrelation(self):
        mc = self.mgcollectionrelation
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]
    def test(self):
        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        ti=[]
        ti.append({'_id': '189123450', 'contacted_pass_user': ['13611111111'], 'contacted_user': ['13611111111']})
        ti.append({'_id': '189123451', 'contacted_pass_user': ['13611111111'], 'contacted_user': ['13611111111']})
        ti.append({'_id': '189123452', 'contacted_pass_user': ['13611111111'], 'contacted_user': ['13611111111']})
        tu=[]
        tu.append('18944444444')
        tu.append('18944444445')
        tu.append('18944444446')
        try:
            if tu:
                print '111'
                crel.update_many({'_id': {'$in': tu}}, {'$addToSet': {'contacted_pass_user': '13611111111', 'contacted_user': '13611111111'}})
                print '1222'
            if ti:
                crel.insert_many(ti)
                print '2323'
        except Exception, e:
            print str(e)
            # traceback.print_exc()
            # print t
            pass


if __name__ == '__main__':
    pd = PutData()
    pd.test()
