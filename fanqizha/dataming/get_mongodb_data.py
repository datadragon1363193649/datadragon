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
        self.conny = pm.MongoClient([self.mhosty1], maxPoolSize=10)
        self.conny[dconf.mg_dby].authenticate(dconf.mg_uname, dconf.mg_passwd)
    def get_mg_conn_yellow(self, mc):
        mdb = self.conny[dconf.mg_dby]
        return mdb[mc]


    # 老版银行数据，特征数量很少
    def get_featuredata_phn_bank(self, phnfilename):
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
    # 带有目标值数据
    def get_featuredata_target_phn(self,phnfilename,name):
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
                    if name not in rl:
                        # print rl
                        continue
                    clist=[]
                    clist.append(rl['_id'])
                    clist.append(phndic[rl['_id']])
                    for fn in feanamelist:
                        if fn in rl[name]:
                            # print fn,str(rl['call'][fn])
                            clist.append(str(rl[name][fn]))
                        else:
                            clist.append(str(-1))
                    if len(clist) < 3:
                        print clist
                        continue
                    cbool=0
                    for li in clist[2:]:
                        if li !='-1':
                            cbool=1
                    if cbool==1:
                        wfp.write('\t'.join(clist)+'\n')
                    # else:
                    #     print clist[:2]
                i=0
                phnlist=[]
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        rs = cf.find({'_id': {'$in': phnlist}})
        # rs = cf.find({'_id':'18601222524'})
        rsl = list(rs)
        calldata = []
        for rl in rsl:
            if name not in rl:
                continue
            clist = []
            clist.append(rl['_id'])
            clist.append(phndic[rl['_id']])
            for fn in feanamelist:
                if fn in rl[name]:
                    # print fn,str(rl['call'][fn])
                    clist.append(str(rl[name][fn]))
                else:
                    clist.append(str(-1))
            if len(clist) < 3:
                continue
            cbool = 0
            for li in clist[2:]:
                if li != '-1':
                    cbool = 1
            if cbool == 1:
                wfp.write('\t'.join(clist) + '\n')
    def get_featuredata_phn(self, phnfilename,name):
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
                # print linelist[1]
                i+=1
                if i % 1000 == 0:
                    rs = cf.find({'_id': {'$in': phnlist}})
                    # rs = cf.find({'_id':'18601222524'})
                    rsl = list(rs)
                    calldata = []
                    # num=0
                    # print rsl
                    for rl in rsl:
                        clist = []
                        # print rl
                        if name not in rl:
                            continue
                        clist.append(rl['_id'])
                        # print clist
                        # clist.append('0')
                        # print rl
                        # if 'call' not in rl:
                        #     continue
                        for fn in feanamelist:
                            #     if 'call' in rl:
                            #         if fn in rl['call']:
                            #             # print fn,str(rl['call'][fn])
                            #             clist.append(str(rl['call'][fn]))
                            if fn in rl[name]:
                                clist.append(str(rl[name][fn]))
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
                if name not in rl:
                    continue
                # clist.append('0')
                for fn in feanamelist:
                    #     if 'call' in rl:
                    #         if fn in rl['call']:
                    #             # print fn,str(rl['call'][fn])
                    #             clist.append(str(rl['call'][fn]))
                    if fn in rl[name]:
                        clist.append(str(rl[name][fn]))
                    else:
                        clist.append(str(-1))
                if len(clist) < 2:
                    continue
                wfp.write('\t'.join(clist) + '\n')
                phnlist = []
    # 之前做实验，call＋tongdun
    def get_featuredata_phn_new(self, phnfilename):
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
    # app
    def get_featuredata_target_phn_app(self, phnfilename):
        fnamelist = ['life_work_distance', 'life_auth_distance', 'work_auth_distance']
        s_file = dconf.config_path + dconf.featurename
        out_file = dconf.data_path + phnfilename + '1'
        s2file = dconf.data_path + phnfilename
        phndic = {}
        with open(s2file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phndic[linelist[0]] = linelist[1]
        phnall = phndic.keys()
        # print len(phnlist)
        feaidlist = []
        feanamelist = []
        wfp = open(out_file, 'w')
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        print len(feaidlist)
        i = 0
        phnlist = []
        for phn in phnall:
            phnlist.append(phn)
            i += 1
            if i > 5000:
                cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
                rs = cf.find({'_id': {'$in': phnlist}})
                # rs = cf.find({'_id':'18601222524'})
                rsl = list(rs)
                calldata = []
                for rl in rsl:
                    if 'app' not in rl:
                        continue
                    clist = []
                    clist.append(rl['_id'])
                    clist.append(phndic[rl['_id']])
                    for fn in feanamelist:
                        if fn in fnamelist:
                            if 'call' not in rl:
                                continue
                            if fn in rl['call']:
                                # print fn,str(rl['call'][fn])
                                clist.append(str(rl['call'][fn]))
                            else:
                                clist.append(str(-1))
                        elif fn in rl['app']:
                            # print fn,str(rl['call'][fn])
                            clist.append(str(rl['app'][fn]))
                        else:
                            clist.append(str(-1))
                    if len(clist) < 3:
                        continue
                    cbool = 0
                    for li in clist[2:]:
                        if li != '-1':
                            cbool = 1
                    if cbool == 1:
                        wfp.write('\t'.join(clist) + '\n')
                i = 0
                phnlist = []
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        rs = cf.find({'_id': {'$in': phnlist}})
        # rs = cf.find({'_id':'18601222524'})
        rsl = list(rs)
        calldata = []
        for rl in rsl:
            if 'app' not in rl:
                continue
            clist = []
            clist.append(rl['_id'])
            clist.append(phndic[rl['_id']])
            for fn in feanamelist:
                if fn in fnamelist:
                    if 'call' not in rl:
                        continue
                    if fn in rl['call']:
                        # print fn,str(rl['call'][fn])
                        clist.append(str(rl['call'][fn]))
                    else:
                        clist.append(str(-1))
                elif fn in rl['app']:
                    # print fn,str(rl['call'][fn])
                    clist.append(str(rl['app'][fn]))
                else:
                    clist.append(str(-1))
            if len(clist) < 3:
                continue
            cbool = 0
            for li in clist[2:]:
                if li != '-1':
                    cbool = 1
            if cbool == 1:
                wfp.write('\t'.join(clist) + '\n')
    def get_featuredata_test_phn_app(self,phnfilename):
        fnamelist=['life_work_distance','life_auth_distance','work_auth_distance']
        s_file= dconf.config_path+dconf.featurename
        out_file=dconf.data_path+phnfilename+'1'
        s2file=dconf.data_path+phnfilename
        phndic={}
        with open(s2file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                phndic[linelist[1]]=0
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
                    if 'app' not in rl:
                        continue
                    clist=[]
                    clist.append(rl['_id'])
                    # clist.append(phndic[rl['_id']])
                    for fn in feanamelist:
                        if fn in fnamelist:
                            if 'call' not in rl:
                                clist.append(str(0))
                                continue
                            if fn in rl['call']:
                                # print fn,str(rl['call'][fn])
                                clist.append(str(rl['call'][fn]))
                            else:
                                clist.append(str(0))
                        elif fn in rl['app']:
                            # print fn,str(rl['call'][fn])
                            clist.append(str(rl['app'][fn]))
                        else:
                            clist.append(str(-1))
                    if len(clist) < 3:
                        continue
                    cbool=0
                    for li in clist[2:]:
                        if li !='-1':
                            cbool=1
                    if cbool==1:
                        wfp.write('\t'.join(clist)+'\n')
                i=0
                phnlist=[]
        cf = self.get_mg_conn_yellow(dconf.mg_coll_feature)
        rs = cf.find({'_id': {'$in': phnlist}})
        # rs = cf.find({'_id':'18601222524'})
        rsl = list(rs)
        calldata = []
        for rl in rsl:
            if 'app' not in rl:
                continue
            clist = []
            clist.append(rl['_id'])
            # clist.append(phndic[rl['_id']])
            for fn in feanamelist:
                if fn in fnamelist:
                    if 'call' not in rl:
                        clist.append(str(0))
                        continue
                    if fn in rl['call']:
                        # print fn,str(rl['call'][fn])
                        clist.append(str(rl['call'][fn]))
                    else:
                        clist.append(str(0))
                elif fn in rl['app']:
                    # print fn,str(rl['call'][fn])
                    clist.append(str(rl['app'][fn]))
                else:
                    clist.append(str(-1))
            if len(clist) < 3:
                continue
            cbool = 0
            for li in clist[2:]:
                if li != '-1':
                    cbool = 1
            if cbool == 1:
                wfp.write('\t'.join(clist) + '\n')


if __name__ == '__main__':
    tr=sys.argv[1]
    input_file = sys.argv[2]
    m=Mongedbdata()
    # app数据有一部分在call里边
    if tr=='targetapp':
        m.get_featuredata_target_phn_app(input_file)
    elif tr=='app':
        m.get_featuredata_test_phn_app(input_file)

    # 带有目标值获取数据
    elif tr=='targetcall':
        m.get_featuredata_target_phn(input_file,'call')
    elif tr == 'targettongdun':
        m.get_featuredata_target_phn(input_file, 'tongdun')
    elif tr == 'targettaobao':
        m.get_featuredata_target_phn(input_file, 'taobao')
    elif tr == 'targetbank':
        m.get_featuredata_target_phn(input_file, 'bank')

    # 没有目标值获取数据
    elif tr=='call':
        m.get_featuredata_phn(input_file,'call')
    elif tr=='tongdun':
        m.get_featuredata_phn(input_file,'tongdun')
    elif tr=='taobao':
        m.get_featuredata_phn(input_file,'taobao')
    elif tr=='bank':
        m.get_featuredata_phn(input_file,'bank')

    elif tr=='new':
        m.get_featuredata_phn_new(input_file)
