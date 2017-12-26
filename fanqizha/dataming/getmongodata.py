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
    def getfeaturedata_phn(self,phnfilename):
        s_file= dconf.config_path+dconf.featurename
        out_file=dconf.data_path+phnfilename+'1'
        s2file=dconf.data_path+phnfilename
        phnlist=[]
        with open(s2file,'r') as fp:
            for line in fp:
                linelist=line.strip().split('\t')
                phnlist.append(linelist[0])
        # print len(phnlist)
        feaidlist=[]
        feanamelist=[]
        wfp=open(out_file,'w')
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        print len(feaidlist)
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        phl=[]
        i=0
        for phn in phnlist:
            if i <10000:
                phl.append(phn)
                i+=1
            else:
                print len(phl)
                rs = cf.find({'_id': {'$in': phl}})
                # rs = cf.find({'_id':'18601222524'})
                rsl = list(rs)
                calldata = []
                for rl in rsl:
                    clist = []
                    clist.append(rl['_id'])
                    # if 'bank' not in rl:
                    #     continue
                    for fn in feanamelist:
                        if 'call' in rl:
                            if fn in rl['call']:
                                # print fn,str(rl['call'][fn])
                                clist.append(str(rl['call'][fn]))
                            elif 'bank' in rl:
                                    if fn in rl['bank']:
                                        clist.append(str(rl['bank'][fn]))
                                    elif 'credit_behavior' in rl:
                                        if fn in rl['credit_behavior']:
                                            clist.append(str(rl['credit_behavior'][fn]))
                                        else:
                                            clist.append(str(-1))
                    if len(clist)>2:
                        wfp.write('\t'.join(clist) + '\n')
                phl=[]
                i=0
        print phl
        rs = cf.find({'_id': {'$in': phl}})
        # rs = cf.find({'_id':'18601222524'})
        rsl = list(rs)
        calldata = []
        for rl in rsl:
            clist = []
            clist.append(rl['_id'])
            # if 'bank' not in rl:
            #     continue
            for fn in feanamelist:
                # if 'call' in rl:
                #     if fn in rl['call']:
                #         # print fn,str(rl['call'][fn])
                #         clist.append(str(rl['call'][fn]))
                #     elif 'bank' in rl:
                if 'bank' in rl:
                            if fn in rl['bank']:
                                clist.append(str(rl['bank'][fn]))
                            elif 'credit_behavior' in rl:
                                if fn in rl['credit_behavior']:
                                    clist.append(str(rl['credit_behavior'][fn]))
                                else:
                                    clist.append(str(-1))
            if len(clist) > 2:
                wfp.write('\t'.join(clist) + '\n')

        # num=0


    def getfeaturedata_phn_bank(self, phnfilename):
        s_file = dconf.config_path + dconf.featurename
        out_file = dconf.data_path + phnfilename + '1'
        s2file = dconf.data_path + phnfilename
        phnlist = []
        with open(s2file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phnlist.append(linelist[0])
        # print len(phnlist)
        feaidlist = []
        feanamelist = []
        wfp = open(out_file, 'w')
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        # print len(feaidlist)
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        rs = cf.find({'_id': {'$in': phnlist}})
        # rs = cf.find({'_id':'18601222524'})
        rsl = list(rs)
        calldata = []
        # num=0
        for rl in rsl:
            clist = []
            clist.append(rl['_id'])
            if 'bank' not in rl:
                continue
            for fn in feanamelist:
            #     if 'call' in rl:
            #         if fn in rl['call']:
            #             # print fn,str(rl['call'][fn])
            #             clist.append(str(rl['call'][fn]))
                    if 'bank' in rl:
                        if fn in rl['bank']:
                            clist.append(str(rl['bank'][fn]))
                        elif 'credit_behavior' in rl:
                            if fn in rl['credit_behavior']:
                                clist.append(str(rl['credit_behavior'][fn]))
                            # else:
                            #     clist.append(str(-1))

            wfp.write('\t'.join(clist) + '\n')

        # print num
    def getfeaturedata_target_phn(self,phnfilename):
        s_file= dconf.config_path+dconf.featurename
        out_file=dconf.data_path+phnfilename+'1'
        s2file=dconf.data_path+phnfilename
        phndic={}
        with open(s2file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                phndic[linelist[0]]=linelist[1]
        phnall=phndic.keys()
        # print len(phnlist)
        feaidlist=[]
        feanamelist=[]
        wfp=open(out_file,'w')
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        print len(feaidlist)
        i=0
        phnlist=[]
        for phn in phnall:
            phnlist.append(phn)
            i+=1
            if i>5000:
                cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
                rs = cf.find({'_id': {'$in': phnlist}})
                # rs = cf.find({'_id':'18601222524'})
                rsl=list(rs)
                calldata=[]
                for rl in rsl:
                    if 'call' not in rl:
                        continue
                    clist=[]
                    clist.append(rl['_id'])
                    clist.append(phndic[rl['_id']])
                    for fn in feanamelist:
                        if fn in rl['call']:
                            # print fn,str(rl['call'][fn])
                            clist.append(str(rl['call'][fn]))
                        else:
                            clist.append(str(-1))
                    if len(clist) < 2:
                        continue
                    wfp.write('\t'.join(clist)+'\n')
                i=0
                phnlist=[]
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        rs = cf.find({'_id': {'$in': phnlist}})
        # rs = cf.find({'_id':'18601222524'})
        rsl = list(rs)
        calldata = []
        for rl in rsl:
            if 'call' not in rl:
                continue
            clist = []
            clist.append(rl['_id'])
            clist.append(phndic[rl['_id']])
            for fn in feanamelist:
                if fn in rl['call']:
                    # print fn,str(rl['call'][fn])
                    clist.append(str(rl['call'][fn]))
                else:
                    clist.append(str(-1))
            if len(clist) < 2:
                continue
            wfp.write('\t'.join(clist) + '\n')
    def getfeaturedata_phn_call(self, phnfilename):
        s_file = dconf.config_path + dconf.featurename
        out_file = dconf.data_path + phnfilename + '1'
        s2file = dconf.data_path + phnfilename
        phnlist = []

        # print len(phnlist)
        feaidlist = []
        feanamelist = []
        wfp = open(out_file, 'w')
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                # print linelist
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        print len(feaidlist)
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        i=0
        with open(s2file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phnlist.append(linelist[0])
                i+=1
                if i % 1000 == 0:
                    rs = cf.find({'_id': {'$in': phnlist}})
                    # rs = cf.find({'_id':'18601222524'})
                    rsl = list(rs)
                    calldata = []
                    # num=0
                    for rl in rsl:
                        clist = []
                        if 'call' not in rl:
                            continue
                        clist.append(rl['_id'])
                        # clist.append('0')
                        # print rl
                        # if 'call' not in rl:
                        #     continue
                        for fn in feanamelist:
                            #     if 'call' in rl:
                            #         if fn in rl['call']:
                            #             # print fn,str(rl['call'][fn])
                            #             clist.append(str(rl['call'][fn]))
                            if fn in rl['call']:
                                clist.append(str(rl['call'][fn]))
                            else:
                                clist.append(str(-1))
                        # print clist
                        if len(clist) < 2:
                            continue
                        wfp.write('\t'.join(clist) + '\n')
                        phnlist=[]
                    print i
        if i % 1000 != 0:
            rs = cf.find({'_id': {'$in': phnlist}})
            # rs = cf.find({'_id':'18601222524'})
            rsl = list(rs)
            calldata = []
            # num=0
            for rl in rsl:
                clist = []
                clist.append(rl['_id'])
                if 'call' not in rl:
                    continue
                # clist.append('0')
                for fn in feanamelist:
                    #     if 'call' in rl:
                    #         if fn in rl['call']:
                    #             # print fn,str(rl['call'][fn])
                    #             clist.append(str(rl['call'][fn]))
                    if fn in rl['call']:
                        clist.append(str(rl['call'][fn]))
                    else:
                        clist.append(str(-1))
                if len(clist) < 2:
                    continue
                wfp.write('\t'.join(clist) + '\n')
                phnlist = []

    def getfeaturedata_phn_new(self, phnfilename):
        s_file = dconf.config_path + dconf.featurename
        out_file = dconf.data_path + phnfilename + '1'
        s2file = dconf.data_path + phnfilename
        phnlist = []

        # print len(phnlist)
        feaidlist = []
        feanamelist = []
        wfp = open(out_file, 'w')
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                # print linelist
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        print len(feaidlist)
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        i = 0
        with open(s2file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phnlist.append(linelist[0])
                i += 1
                if i % 1000 == 0:
                    rs = cf.find({'_id': {'$in': phnlist}})
                    # rs = cf.find({'_id':'18601222524'})
                    rsl = list(rs)
                    calldata = []
                    # num=0
                    for rl in rsl:
                        clist = []
                        clist.append(rl['_id'])
                        # clist.append('0')
                        # print rl
                        # if 'call' not in rl:
                        #     continue
                        for fn in feanamelist:
                            if 'call' in rl:
                                if fn in rl['call']:
                                    # print fn,str(rl['call'][fn])
                                    clist.append(str(rl['call'][fn]))
                                elif 'tongdun' in rl:
                                    if fn in rl['tongdun']:
                                        clist.append(str(rl['tongdun'][fn]))
                            # if fn in rl['call']:
                            #     clist.append(str(rl['call'][fn]))
                            # else:
                            #     clist.append(str(-1))
                        print len(clist)
                        if len(clist) < 13:
                            # print '111'
                            continue
                        wfp.write('\t'.join(clist) + '\n')
                        phnlist = []
                    print i
        if i % 1000 != 0:
            rs = cf.find({'_id': {'$in': phnlist}})
            # rs = cf.find({'_id':'18601222524'})
            rsl = list(rs)
            calldata = []
            # num=0
            for rl in rsl:
                clist = []
                clist.append(rl['_id'])
                # clist.append('0')
                if 'call' not in rl:
                    continue
                for fn in feanamelist:
                    if 'call' in rl:
                        if fn in rl['call']:
                            # print fn,str(rl['call'][fn])
                            clist.append(str(rl['call'][fn]))
                        elif 'tongdun' in rl:
                            if fn in rl['tongdun']:
                                clist.append(str(rl['tongdun'][fn]))
                            # if fn in rl['call']:
                            #     clist.append(str(rl['call'][fn]))
                            # else:
                            #     clist.append(str(-1))
                            # print clist
                if len(clist) < 13:
                    continue
                wfp.write('\t'.join(clist) + '\n')
                phnlist = []
    def getfeaturedata_phn_lin(self, phnfilename):
        s_file = dconf.config_path + dconf.featurename
        out_file = dconf.data_path + phnfilename + '_notrain'
        s2file = dconf.data_path + phnfilename
        phnlist = []
        print len(phnlist)
        feaidlist = []
        feanamelist = []
        wfp = open(out_file, 'w')
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        print len(feaidlist)
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        i=0
        with open(s2file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phnlist.append(linelist[1])
                i+=1
                if i % 1000 == 0:
                    rs = cf.find({'_id': {'$in': phnlist}})
                    # rs = cf.find({'_id':'18601222524'})
                    rsl = list(rs)
                    calldata = []
                    # num=0
                    for rl in rsl:
                        clist = []

                        # clist.append('0')
                        if 'call' not in rl:
                            continue
                        clist.append(rl['_id'])
                        # for fn in feanamelist:
                        #     #     if 'call' in rl:
                        #     #         if fn in rl['call']:
                        #     #             # print fn,str(rl['call'][fn])
                        #     #             clist.append(str(rl['call'][fn]))
                        #     if 'call' in rl:
                        #         if fn in rl['call']:
                        #             clist.append(str(rl['call'][fn]))
                        #             # else:
                        #             #     clist.append(str(-1))
                        # if len(clist) < 2:
                        #     continue
                        wfp.write('\t'.join(clist) + '\n')
                        phnlist=[]
                    print i
        if i % 1000 != 0:
            rs = cf.find({'_id': {'$in': phnlist}})
            # rs = cf.find({'_id':'18601222524'})
            rsl = list(rs)
            calldata = []
            # num=0
            for rl in rsl:
                clist = []

                # clist.append('0')
                if 'call' not in rl:
                    continue
                clist.append(rl['_id'])
                # for fn in feanamelist:
                #     #     if 'call' in rl:
                #     #         if fn in rl['call']:
                #     #             # print fn,str(rl['call'][fn])
                #     #             clist.append(str(rl['call'][fn]))
                #     if 'call' in rl:
                #         if fn in rl['call']:
                #             clist.append(str(rl['call'][fn]))
                #             # else:
                #             #     clist.append(str(-1))
                # if len(clist) < 8:
                #     continue
                wfp.write('\t'.join(clist) + '\n')
                phnlist = []


if __name__ == '__main__':
    tr=sys.argv[1]
    input_file = sys.argv[2]
    m=Mongedbdata()
    if tr=='target':
    # m.getfeaturedata()
        m.getfeaturedata_target_phn(input_file)
    elif tr=='bank':
        m.getfeaturedata_phn_bank(input_file)
    elif tr=='all':
        m.getfeaturedata_phn(input_file)
    elif tr=='call':
        m.getfeaturedata_phn_call(input_file)
    elif tr=='new':
        m.getfeaturedata_phn_new(input_file)
    else:
        m.getfeaturedata_phn_lin(input_file)