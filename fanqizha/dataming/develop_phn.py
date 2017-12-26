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
import pandas as pd
import offline_db_conf as dconf
# import MySQLdb
import logging
import shutil
import math
reload(sys)
sys.setdefaultencoding('utf-8')

class PhnData(object):
    def __init__(self):
        self.mhost1 = dconf.mg_hosty1
        self.mhost2 = dconf.mg_hosty2
        self.mreplicat_set = dconf.mg_replicat_set
        self.mgdb=dconf.mg_dby
        self.mgcollection=dconf.mg_collection
        self.mgcollectionrelation = dconf.mg_coll_relation
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.mgconn = None
        self.init_mg_conn()
        # self.mysqlhost=dconf.mysql_host
        # self.mysqlport=dconf.mysql_port
        # self.mysqluser=dconf.mysql_user
        # self.mysqlpasswd=dconf.mysql_passwd
        # self.mysqldb=dconf.mysql_db
        # self.mysqlconn=None
        # self.init_mysql_conn()
        self.allphndic={}
        self.relationdic={}
        self.featurelist=[]
        self.commonalitydic={}
        # self.daylist=dconf.daylist

    # def init_mysql_conn(self):
    #     self.mysqlconn= MySQLdb.connect(host=self.mysqlhost, port=self.mysqlport,
    #                                     user=self.mysqluser, passwd=self.mysqlpasswd,
    #                            db=self.mysqldb, charset="utf8")
    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost2], maxPoolSize=10)
        self.mgconn[dconf.mg_dby].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self,mc):
        # mc = self.mgcollection
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

    def fdetail(self,phn,wfp):
        phndic={}
        c = self.get_mg_conn()
        re = c.find({'_id': phn})
        if re.count()>=1:
            self.allphndic[phn] = '-1'
            rsl=list(re)
            if 'contacts' in rsl[0]:
                contactslist=rsl[0]['contacts']
                for call in contactslist:
                    if 'phone' in call:
                        phnlist=call['phone']
                        for phnl in phnlist:
                            p = phnl
                            p = p.strip()
                            p = p.replace(' ', '').replace('+86', '').replace('-', ''). \
                                replace('*86', '').replace(';', '')
                            if len(p) == 13 and p[:2] == '86':
                                p = p[2:]
                            if len(p) == 11 and p[0]=='1':
                                phndic.setdefault(p[:7],0)
                                phndic[p[:7]]+=1

                                # self.relationdic.setdefault(p,{})
                                # self.relationdic[p].setdefault(phn, 0)
                                # self.allphndic[p] = '-1'
            for phnd in phndic:
                if phndic[phnd]>5:
                    wfp.write(phn+','+phnd+','+str(phndic[phnd])+'\n')

    def put_data_relation(self, fphn,wfp):
        i = 0
        for phn in fphn:
            i += 1
            if i % 100 == 0:
                time.sleep(1)
            self.fdetail(phn,wfp)


    def get_data_tag_all(self,tat):
        out_file=dconf.data_path+'develop_7'+tat
        wfp=open(out_file,'w')
        s_file=dconf.data_path+tat
        phncontent=[]
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                phncontent.append(linelist[0])
        i=0
        buffer=[]
        for fli in phncontent:
            buffer.append(fli)
            i += 1
            if i % 1000 == 0:
                self.put_data_relation(buffer,wfp)
                buffer = []
        if i % 1000 != 0:
            self.put_data_relation(buffer,wfp)
    def mergedata(self,tat):
        s_file=dconf.data_path+'develop1'+tat
        s2_file=dconf.data_path+tat
        out_file=dconf.data_path+'develop_1merge'+tat+'.csv'
        # wfp=open(out_file,'w')
        scoredic = {}
        with open(s2_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                scoredic[linelist[0]] = linelist
        # numdic={}
        strn=''
        l = ''
        numlist=[]
        maxnum=0
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                # numdic.setdefault(linelist[0],'')
                r=linelist[0]
                # strn = linelist[1] + ':' + linelist[2]
                if r==l:
                        strn=strn+'^'+linelist[1]+':'+linelist[2]
                        if int(linelist[2]) > maxnum:
                            maxnum = int(linelist[2])
                else:
                    if len(strn)<2:
                        if int(linelist[2])>maxnum:
                            maxnum=int(linelist[2])
                        strn = linelist[1] + ':' + linelist[2]
                        l=r
                    else:
                        if l in scoredic:
                            nl=scoredic[l]
                            nl.append(maxnum)
                            nl.append(strn)
                            numlist.append(nl)
                            # wfp.write(','.join(scoredic[l])+','+strn+'\n')
                            l=r
                            strn=linelist[1] + ':' + linelist[2]
                            maxnum = int(linelist[2])
        nldf=pd.DataFrame(numlist,columns=['phn','daytime','myscore','tdscore','overday','maxnum','phnnum'])
        nldf.to_csv(out_file,index=False)
    def put_data_bill(self,fphn):
        i = 0
        initialdate = '2016-07-15 00:00:00'
        initialdate = datetime.datetime.strptime(initialdate, "%Y-%m-%d %H:%M:%S")

        for phn in fphn:
            i += 1
            if i % 100 == 0:
                time.sleep(1)
            c = self.get_mg_conn(dconf.mg_collection)
            re = c.find({'_id': phn})
            if re.count() >= 1:
                rsl = list(re)
                if 'call_details' in rsl[0]:
                    call_details = rsl[0]['call_details']
                    try:
                        innetdate = call_details['innet_date']
                        innetdate = datetime.datetime.strptime(innetdate, "%Y-%m-%d %H:%M:%S")
                        # print phn,(innetdate - initialdate).seconds
                        # if  innetdate>initialdate:
                        if 'bill_info' in call_details:
                            sum1 = 0.0
                            sum2 = 0.0
                            N = 0
                            billlist = []
                            billdic = {}
                            for bi in call_details['bill_info']:
                                if 'callPay' in bi:
                                    if bi['time']=='201707':
                                        continue
                                    billlist.append(float(bi['callPay']))
                                    sum1 += float(bi['callPay'])
                                    sum2 += float(bi['callPay']) ** 2
                                    N += 1
                            if N > 0:
                                means = sum1 / N
                                variance = sum2 / N - means ** 2
                                # for j in range(0, len(billlist)):
                                #     if j == 0:
                                #         billdic[j] = 0
                                #     for i in range(0, j):
                                #         differs = abs(billlist[j] - billlist[i])
                                #         billdic.setdefault(j, 0)
                                #         # print j,i,differs
                                #         billdic[j] += differs
                                #         billdic[i] += differs
                                # # print billdic
                                # bsum1 = 0.0
                                # bsum2 = 0.0
                                # for li in billdic:
                                #     bsum1 += float(billdic[li])*1.0/len(billdic)
                                #     bsum2 += (float(billdic[li])*1.0/len(billdic)) ** 2
                                # bmeans = bsum1 / N
                                # differ_variance = bsum2 / N - bmeans ** 2
                                # print self.allphndic[phn],variance,differ_variance
                                alist=self.allphndic[phn]
                                alist.append(math.sqrt(variance))
                                alist.append(means)
                                self.featurelist.append(alist)
                    except:
                        pass
    def bill_info(self,tat):
        out_file = dconf.data_path + 'bill_' + tat+'.csv'
        # wfp = open(out_file, 'w')
        s_file = dconf.data_path + tat
        phncontent ={}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                if float(linelist[3])>-1:
                    self.allphndic[linelist[0]]=linelist
        i = 0
        buffer = []
        for fli in self.allphndic:
            buffer.append(fli)
            i += 1
            if i % 1000 == 0:
                self.put_data_bill(buffer)
                buffer = []
                print i
        if i % 1000 != 0:
            self.put_data_bill(buffer)
        nldf = pd.DataFrame(self.featurelist, columns=['phn', 'daytime', 'myscore', 'overday', 'variance', 'means'])
        nldf.to_csv(out_file, index=False)
    def fdetail_715(self,phn,wfp):
        phndic={}
        # print phn
        c = self.get_mg_conn()
        re = c.find({'_id': phn})
        if re.count()>=1:
            self.allphndic[phn] = '-1'
            rsl=list(re)
            if 'call_details' in rsl[0]:
                # contactslist=rsl[0]['call_details']
                if 'call_info' in rsl[0]['call_details']:
                    pcall_info = rsl[0]['call_details']['call_info']
                    for call in pcall_info:
                        # print call
                        if 'phone_num' in call:
                            p = call['phone_num']
                            p = p.strip()
                            p = p.replace(' ', '').replace('+86', '').replace('-', ''). \
                                replace('*86', '').replace(';', '')
                            if len(p) == 13 and p[:2] == '86':
                                p = p[2:]
                            if len(p) == 11 and p[0]=='1':
                                phndic[p]=0

                                # self.relationdic.setdefault(p,{})
                                # self.relationdic[p].setdefault(phn, 0)
                                # self.allphndic[p] = '-1'
            phnstr=''
            # print len(phndic)
            for phnd in phndic:
                if len(phnstr)<2:
                    phnstr=phnd
                else:
                    phnstr=phnstr+'^'+phnd
            wfp.write(phn+','+phnstr+'\n')
    def before_some_days(self, d, delta):
        return (datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S") -
                datetime.timedelta(days=delta)).strftime("%Y-%m-%d %H:%M:%S")
    def fdetail_6(self,phn,wfp):
        phndic={}
        # print phn
        c = self.get_mg_conn()
        re = c.find({'_id': phn})
        if re.count()>=1:
            self.allphndic[phn] = '-1'
            rsl=list(re)
            first_call_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # print first_call_time
            anchor1 = self.before_some_days(first_call_time, 30)
            if 'call_details' in rsl[0]:
                # contactslist=rsl[0]['call_details']
                if 'call_info' in rsl[0]['call_details']:
                    try:
                        # max_comm_plac = call_details['max_comm_plac']
                        first_call_time = rsl[0]['call_details']['call_info'][0]['start_time']
                        anchor1 = self.before_some_days(first_call_time, 30)
                        # print anchor1
                        # anchor2 = self.before_some_days(first_call_time, 90)
                    except :
                        pass
                    # print anchor1
                    pcall_info = rsl[0]['call_details']['call_info']
                    for call in pcall_info:
                        # print call
                        try:
                            st = call['start_time']
                            # print st
                            com_mode = call['comm_mode']
                            if st>anchor1:
                                # print st
                                if 'phone_num' in call:

                                    if com_mode=='被叫':
                                        # print com_mode
                                        # print anchor1,st
                                        p = call['phone_num']
                                        # print p
                                        p = p.strip()
                                        p = p.replace(' ', '').replace('+86', '').replace('-', ''). \
                                            replace('*86', '').replace(';', '')
                                        if len(p) == 13 and p[:2] == '86':
                                            p = p[2:]
                                        if len(p) >5:
                                            phndic[p]=0
                        except:
                            pass
            # print len(phndic)
            if len(phndic)>0:
                for phn in phndic:
                    self.commonalitydic.setdefault(phn,0)
                    self.commonalitydic[phn]+=1
            # phnstr=''
            # # print len(phndic)
            # for phnd in phndic:
            #     if len(phnstr)<2:
            #         phnstr=phnd
            #     else:
            #         phnstr=phnstr+'^'+phnd
            # wfp.write(phn+','+phnstr+'\n')
    def fdetail_common(self,phn):
        c = self.get_mg_conn(self.mgcollectionrelation)
        re = c.find({'_id': phn})
        if re.count()>=1:
            rsl = list(re)
            if 'contacted_user' in rsl[0]:
                cuser=rsl[0]['contacted_user']
                for cu in cuser:
                    if len(cu)>5:
                        self.allphndic.setdefault(cu,0)
                        self.allphndic[cu]+=1
    def put_data_detail(self, fphn,tfl):
        i = 0
        for phn in fphn:
            i += 1
            if i % 100 == 0:
                time.sleep(1)
            if tfl=='6':
                self.fdetail_6(phn)
            elif tfl=='common':
                self.fdetail_common(phn)
            else:
                self.fdetail_715(phn)

    def get_data_tag_715(self, tat,tfl):
        out_file = dconf.data_path + 'common_' + tat
        wfp = open(out_file, 'w')
        s_file = dconf.data_path + tat
        phncontent = []
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phncontent.append(linelist[2])
        print phncontent
        i = 0
        buffer = []
        for fli in phncontent:
            buffer.append(fli)
            i += 1
            if i % 1000 == 0:
                self.put_data_detail(buffer, wfp,tfl)
                buffer = []
                print i
        if i % 1000 != 0:
            self.put_data_detail(buffer, wfp,tfl)
        commonlist=[]
        for common in self.commonalitydic:
            commonlist.append([common,self.commonalitydic[common]])
        commondf=pd.DataFrame(commonlist,columns=['phn','daytime','myscore','overday','num'])
        commondf.to_csv(out_file,index=False)
    def get_data_mate(self, tat,tfl):
        out_file = dconf.data_path + 'phnnumdf_715.csv'
        # wfp = open(out_file, 'w')
        s_file = dconf.data_path + tat
        s2_file=dconf.data_path +'phn_score_715'
        s3_file=dconf.data_path+'score_715'
        scoredic={}
        with open(s3_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                scoredic[linelist[0]]=linelist
        phncontent = []
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phncontent.append(linelist[0])
        print phncontent
        i = 0
        buffer = []
        for fli in phncontent:
            buffer.append(fli)
            i += 1
            if i % 1000 == 0:
                self.put_data_detail(buffer, tfl)
                buffer = []
                print i
        if i % 1000 != 0:
            self.put_data_detail(buffer, tfl)
        phnnumlist=[]
        with open(s2_file, 'r') as fp:
            for line in fp:
                num=0
                linelist = line.strip().split(',')
                phnlist=linelist[1].strip().split('^')
                if linelist[0] in scoredic:
                    for phn in phnlist:
                        if phn in self.allphndic:
                            num+=self.allphndic[phn]
                    nlist=scoredic[linelist[0]]
                    nlist.append(num)
                    phnnumlist.append(nlist)
        phnnumdf=pd.DataFrame(phnnumlist,columns=['phn','daytime','myscore','overday','num'])
        phnnumdf.to_csv(out_file,index=False)

        # commonlist=[]
        # for common in self.commonalitydic:
        #     commonlist.append([common,self.commonalitydic[common]])
        # commondf=pd.DataFrame(commonlist,columns=['phn','num'])
        # commondf.to_csv(out_file,index=False)

    def get_maxcom_num(self):
        out_file = dconf.data_path + 'proportionnumdf_715.csv'
        # wfp = open(out_file, 'w')
        s_file = dconf.data_path + tat
        # s2_file = dconf.data_path + 'phn_score_715'
        s3_file = dconf.data_path + 'score_715'
        scoredic = {}
        with open(s3_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                if float(linelist[3])>-1:
                    # print linelist
                    scoredic[linelist[0]] = linelist
        # phncontent = []
        # with open(s_file, 'r') as fp:
        #     for line in fp:
        #         linelist = line.strip().split(',')
        #         phncontent.append(linelist[0])
        # maxnocom = -1
        phnnumlist = []
        cn = self.get_mg_conn(dconf.mg_collection)
        for phn in scoredic:
            cinfo = cn.find({'_id': phn})
            cinfo=list(cinfo)
            if not cinfo:
                pass
            if 'call_details' in cinfo[0] and 'contacts' in cinfo[0]:
                # print '111'
                maxdays = -1
                contactnum=len(cinfo[0]['contacts'])
                ud=cinfo[0]['call_details']
                if 'call_info' not in ud:
                    continue
                callinfo = ud['call_info']
                phndic={}
                stbegin='2017-07-15 12:00:00'
                stbegin1 = datetime.datetime.strptime(stbegin, "%Y-%m-%d %H:%M:%S")
                stleft = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                stleft = datetime.datetime.strptime(stleft, "%Y-%m-%d %H:%M:%S")
                anchor1 = self.before_some_days(stbegin, 30)
                # print anchor1,'111'
                # anchor1=None
                try:
                    st = callinfo[0]['start_time']
                    # print st
                    stbegin1 = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                    # print stbegin
                    stleft = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                    anchor1 = self.before_some_days(st, 30)
                    # print anchor1
                except :
                    pass
                # print anchor1,'222'
                stend=stleft
                for c in callinfo:
                    try:
                        st = c['start_time']
                        # print st
                        if st == '':
                            continue
                        phndic[c['phone_num']]=0
                        if st > anchor1:
                            stright = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                            days = (stleft - stright).days
                            # print stleft,stbegin, stright,days,phn
                            if days<1:
                                stleft = stright
                                stend=stright
                            else:
                                daysum=(stbegin1 - stleft).days
                                if daysum>maxdays:
                                    maxdays=daysum
                                stleft = stright
                                stbegin1=stright
                        else:
                            break
                    except :
                        pass
                daysum = (stbegin1 - stend).days
                if daysum > maxdays:
                    maxdays = daysum
                contactb=maxdays*1.0/(contactnum+1)
                detailb = maxdays * 1.0 / (len(phndic) + 1)
                numlist=scoredic[phn]
                numlist.append(contactb)
                numlist.append(detailb)
                phnnumlist.append(numlist)
        phnnumdf = pd.DataFrame(phnnumlist, columns=['phn', 'daytime', 'myscore', 'overday', 'contactb','detailb'])
        phnnumdf.to_csv(out_file, index=False)
    def get_differcom_num(self):
        out_file = dconf.data_path + 'differnumdf_715.csv'
        # wfp = open(out_file, 'w')
        s_file = dconf.data_path + tat
        # s2_file = dconf.data_path + 'phn_score_715'
        s3_file = dconf.data_path + 'score_715'
        scoredic = {}
        with open(s3_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                # if float(linelist[3])>-1:
                    # print linelist
                scoredic[linelist[0]] = linelist
        # phncontent = []
        # with open(s_file, 'r') as fp:
        #     for line in fp:
        #         linelist = line.strip().split(',')
        #         phncontent.append(linelist[0])
        # maxnocom = -1
        phnnumlist = []
        cn = self.get_mg_conn(dconf.mg_collection)
        for phn in scoredic:
            cinfo = cn.find({'_id': phn})
            cinfo=list(cinfo)
            if not cinfo:
                pass
            if 'call_details' in cinfo[0] and 'contacts' in cinfo[0]:
                # print '111'
                maxdays = -1
                ud=cinfo[0]['call_details']
                if 'call_info' not in ud:
                    continue
                callinfo = ud['call_info']
                stbegin=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                stbegin = datetime.datetime.strptime(stbegin, "%Y-%m-%d %H:%M:%S")
                stleft = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                stleft = datetime.datetime.strptime(stleft, "%Y-%m-%d %H:%M:%S")
                # anchor1 = self.before_some_days(stbegin, 30)
                # print anchor1,'111'
                # anchor1=None
                try:
                    st = callinfo[0]['start_time']
                    # print st
                    # stbegin = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                    # print stbegin
                    stleft = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                    # anchor1 = self.before_some_days(st, 30)
                    # print anchor1
                except :
                    pass
                # print anchor1,'222'
                # stend=None
                # for c in callinfo:
                #     try:
                #         st = c['start_time']
                #         # print st
                #         if st == '':
                #             continue
                #         if st > anchor1:
                #             stright = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                #             days = (stleft - stright).days
                #             # print stleft,stbegin, stright,days,phn
                #             if days<1:
                #                 stleft = stright
                #                 stend=stright
                #             else:
                #                 daysum=(stbegin - stleft).days
                #                 if daysum>maxdays:
                #                     maxdays=daysum
                #                 stleft = stright
                #                 stbegin=stright
                #         else:
                #             break
                #     except :
                #         pass
                # daysum = (stbegin - stend).days
                # if daysum > maxdays:
                #     maxdays = daysum
                maxdays = (stbegin - stleft).days
                numlist=scoredic[phn]
                numlist.append(maxdays)
                phnnumlist.append(numlist)
        phnnumdf = pd.DataFrame(phnnumlist, columns=['phn', 'daytime', 'myscore', 'overday', 'differnum'])
        phnnumdf.to_csv(out_file, index=False)
    def get_operator_num(self):
        out_file = dconf.data_path + 'operatordf_715.csv'
        # wfp = open(out_file, 'w')
        s_file = dconf.data_path + tat
        # s2_file = dconf.data_path + 'phn_score_715'
        s3_file = dconf.data_path + 'score_715'
        scoredic = {}
        with open(s3_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                if float(linelist[3])>-1:
                    # print linelist
                    scoredic[linelist[0]] = linelist
        # phncontent = []
        # with open(s_file, 'r') as fp:
        #     for line in fp:
        #         linelist = line.strip().split(',')
        #         phncontent.append(linelist[0])
        # maxnocom = -1
        phnnumlist = []
        cn = self.get_mg_conn(dconf.mg_collection)
        for phn in scoredic:
            cinfo = cn.find({'_id': phn})
            cinfo=list(cinfo)
            if not cinfo:
                pass
            if 'call_details' in cinfo[0] and 'contacts' in cinfo[0]:
                # print '111'
                # maxdays = -1
                ud=cinfo[0]['call_details']
                operation_phn=''
                if 'operation_type' in ud:
                    if ud['operation_type']==u'移动':
                        operation_phn=dconf.operator_dic[ud['operation_type']]
                if operation_phn=='':
                    continue
                if 'call_info' not in ud:
                    continue
                callinfo = ud['call_info']
                stbegin='2017-07-15 12:00:00'
                # stbegin1 = datetime.datetime.strptime(stbegin, "%Y-%m-%d %H:%M:%S")
                # stleft = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # stleft = datetime.datetime.strptime(stleft, "%Y-%m-%d %H:%M:%S")
                anchor1 = self.before_some_days(stbegin, 90)
                # print anchor1,'111'
                # anchor1=None
                try:
                    st = callinfo[0]['start_time']
                    # print st
                    # stbegin1 = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                    # print stbegin
                    # stleft = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                    anchor1 = self.before_some_days(st, 90)
                    # print anchor1
                except :
                    pass
                # print anchor1,'222'
                # stend=stleft

                passivitynum=0
                accordnum = 0
                for c in callinfo:
                    try:
                        st = c['start_time']
                        # print st
                        if st == '':
                            continue
                        if st > anchor1:
                            callphn=c['phone_num']
                            if operation_phn in callphn:
                                # phnindex=callphn
                                comm_mode=c['comm_mode']
                                if comm_mode=='被叫':
                                    passivitynum+=1
                                if comm_mode=='主叫':
                                    accordnum+=1

                            # stright = datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                            # days = (stleft - stright).days
                            # # print stleft,stbegin, stright,days,phn
                            # if days<1:
                            #     stleft = stright
                            #     stend=stright
                            # else:
                            #     daysum=(stbegin1 - stleft).days
                            #     if daysum>maxdays:
                            #         maxdays=daysum
                            #     stleft = stright
                            #     stbegin1=stright
                        else:
                            break
                    except :
                        pass
                differnum = accordnum-passivitynum
                # if daysum > maxdays:
                #     maxdays = daysum
                numlist=scoredic[phn]
                numlist.append(differnum)
                phnnumlist.append(numlist)
        phnnumdf = pd.DataFrame(phnnumlist, columns=['phn', 'daytime', 'myscore', 'overday', 'differnum'])
        phnnumdf.to_csv(out_file, index=False)


if __name__ == '__main__':
    p=PhnData()
    tat = 'score_715'
    tfl='common'
    p.get_data_tag_all(tat)
    # p.mergedata(tat)
    # p.bill_info(tat)
    # p.get_data_tag_715(tat,tfl)
    # p.get_data_mate(tat,tfl)
    # p.get_maxcom_num()
    # p.get_differcom_num()
    # p.get_operator_num()