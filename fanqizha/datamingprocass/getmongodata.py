#! /usr/bin/env python
# -*- cocoding: utf-8 -*-

import sys
import os
import time
import datetime
import numpy as np
import math
import json
from scipy import stats
from sklearn.utils.multiclass import type_of_target
from sklearn.decomposition import PCA
import traceback
import pymongo as pm
import offline_db_conf as dconf
import pandas as pd
from numpy import *
from math import*
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
class Mongedbdata(object):
    def __init__(self):
        self.mhosty1 = dconf.mg_hosty2
        self.conny = None
        self.init_mg_conn_yellowpage()
    def init_mg_conn_yellowpage(self):
        # self.conn = pm.MongoClient([self.mhost2],replicaSet=self.mreplicat_set, maxPoolSize = 10)
        self.conny = pm.MongoClient([self.mhosty1], maxPoolSize=10)
        self.conny[dconf.mg_dby].authenticate(dconf.mg_uname, dconf.mg_passwd)

    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost1, self.mhost2],
                                     replicaSet=self.mreplicat_set, maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)
    def get_mg_conn_yellow(self, mc):
        mdb = dconf.mg_dby
        mdb = self.conny[dconf.mg_dby]
        return mdb[mc]
    def getfeaturedata(self):
        s_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
        out_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeature_4'
        # in_file='/Users/ufenqi/Downloads/phnother'
        # phnlist=[]
        # with open(in_file,'r')as fp:
        #     for line in fp:
        #         linelist=line.strip().split(',')
        #         phnlist.append(linelist[0])
        feaidlist=[]
        feanamelist=[]
        wfp=open(out_file,'w')
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                print linelist
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        # rs = cf.find({'$and':[{'call.pass_count_recent_all_ratio':{'$gte':1}},{'attestation_time':{'$exists':False}}]})
        # rs = cf.find({'attestation_time': {'$exists': False}})
        # rs=cf.find({'_id': {'$in': phnlist}})
        rs = cf.find({'attestation_time':'2017/4/1-30'})
        rsl=list(rs)
        print len(rsl)
        calldata=[]
        for rl in rsl:
            if rl['_id']=='15829867455':
                print '1111'
            if 'call' in rl:
                clist=[]
                clist.append(rl['_id'])
                for fn in feanamelist:
                    if fn in rl['call']:
                        clist.append(str(rl['call'][fn]))
                    else:
                        clist.append(str(-1))
                wfp.write('\t'.join(clist)+'\n')
            # calldata.append(clist)

        # print rs.count()
        # print rsl[0]
    def getfeaturedata_phn(self):
        s_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
        out_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeature_4'
        s2file='/Users/ufenqi/Documents/phnall'
        phnlist=[]
        with open(s2file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                phnlist.append(linelist[0])
        feaidlist=[]
        feanamelist=[]
        wfp=open(out_file,'w')
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)

        rs = cf.find({'_id': {'$in': phnlist}})
        # rs = cf.find({'_id':'18601222524'})
        rsl=list(rs)
        calldata=[]
        for rl in rsl:
            clist=[]
            clist.append(rl['_id'])
            for fn in feanamelist:
                if fn in rl['call']:
                    # print fn,str(rl['call'][fn])
                    clist.append(str(rl['call'][fn]))
                else:
                    clist.append(str(-1))
            wfp.write('\t'.join(clist)+'\n')
if __name__ == '__main__':
    m=Mongedbdata()
    # m.getfeaturedata()
    m.getfeaturedata_phn()