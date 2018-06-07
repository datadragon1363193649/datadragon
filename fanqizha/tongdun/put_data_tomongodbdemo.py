#! /usr/bin/env python
# -*- encoding: utf-8 -*-


import os
import sys
import traceback
import json
import pymongo as pm
import datetime
import offline_db_conf as dconf
from math import *
reload(sys)
sys.setdefaultencoding('utf-8')


class PutData(object):
    def __init__(self):
        self.mhost1 = dconf.mg_host1
        self.mhost2 = dconf.mg_host2
        self.mreplicat_set = dconf.mg_replicat_set
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
        self.mgconn = pm.MongoClient([self.mhost2],  maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self):
        mdb = dconf.mg_db
        mc = dconf.mg_collection
        mdb = self.mgconn[dconf.mg_db]
        return mdb[mc]

    def mongodbselect(self):
        s_file = '/Users/ufenqi/Downloads/tongdunSource1.txt'
        output_file = '/Users/ufenqi/Downloads/tongdunSource2.txt'
        wfp = open(output_file, 'w')
        with open(s_file, 'r') as fp:
            for line in fp:
                jsoncontent=None
                line=line.strip()
                linelist = line.split('|')

                print linelist[0]
                c = self.get_mg_conn()
                re = c.find({'_id': linelist[0]})
                if re.count() == 0:
                    line = line + '^'
                else:
                    rsl = list(re)
                    if 'ruleDetail' in rsl[0]:
                        jsoncontent= rsl[0]['ruleDetail']
                        # print rsl[0]['policySet']
                        line=line+'^'+str(jsoncontent)
                    else:
                        line = line + '^'
                wfp.write(line + '\n')

    def test(self):
        output_file = '/Users/ufenqi/Downloads/yanghannew.txt'
        wfp = open(output_file, 'w')
        jsoncontent = None
        c = self.get_mg_conn()
        re = c.find({'_id': '13697039290|APP'})
        rsl = list(re)
        print rsl
        jsoncontent = rsl[0]['policySet']
        print rsl[0]['policySet']
        line = 'a'+'|'+jsoncontent
        wfp.write(line + '\n')
    def bagging(self):
        in_file='/Users/ufenqi/Documents/dataming/phn_risk/phn_lay_dot/dataset/rule_phn'
        output_file = '/Users/ufenqi/Documents/dataming/phn_risk/phn_lay_dot/dataset/rule_phn_baging'
        uid_list=[]
        wfp=open(output_file,'w')
        with open(in_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                uid_list.append(linelist[0])
        print uid_list
        c = self.get_mg_conn()
        re = c.find({'_id': {'$in': uid_list}})
        print re.count()
        for bi in re:
            print bi
            if 'prob' in bi:
                wfp.write(str(bi['_id'])+','+str(bi['prob']['bagging'])+'\n')
            # res.add(i['_id'])
        wfp.close()
    def tongdun(self):
        in_file='/Users/ufenqi/Documents/dataming/phn_risk/dataset/xxz_id_15_27'
        output_file = '/Users/ufenqi/Documents/dataming/phn_risk/dataset/xxz_id_15_27_td'
        uid_list=[]
        uid_dic={}
        wfp=open(output_file,'w')
        with open(in_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                uid_dic[linelist[0]]=linelist[1]
                uid_list.append(linelist[0])
        c = self.get_mg_conn()
        re = c.find({'_id': {'$in': uid_list}})
        for bi in re:
            wfp.write(str(bi['_id'])+','+uid_dic[bi['_id']]+','+str(bi['finalScore'])+'\n')
            # res.add(i['_id'])
        wfp.close()
    def overday_num(self):
        in_file='/Users/ufenqi/Documents/dataming/phn_risk/dataset/xxz_task_d'
        output_file = '/Users/ufenqi/Documents/dataming/phn_risk/dataset/xxz_task_m_score'
        uid_list=[]
        uid_dic={}
        wfp=open(output_file,'w')
        with open(in_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                uid_dic[linelist[0]]=linelist[1]
                uid_list.append(linelist[0])
        c = self.get_mg_conn()
        re = c.find({'_id': {'$in': uid_list}})
        for bi in re:
            wfp.write(str(bi['_id'])+','+uid_dic[bi['_id']]+',1,'+str(bi['overdueNumM'])+'\n')
            # res.add(i['_id'])
        wfp.close()
    def relation(self):
        # in_file='/Users/ufenqi/Downloads/content.txt'
        # phn_list=[]
        # with open(in_file,'r') as fp:
        #     for line in fp:
        #         linelist=line.strip().split(',')
        #         if len(linelist[0])>12:
        #             continue
        #         phn_list.append(linelist[0])
        c = self.get_mg_conn()
        re = c.find({'is_black': True})
        # print len(re)
        p_list=[]
        p2_list=[]
        i=0
        for bi in re:
            i+=1
        print i
        # for bi in re:
        #     p_list.append(bi['_id'])
        #     if 'is_refuse' in bi or 'is_pass' in bi:
        #         # print bi['_id'],bi['is_black']
        #         p2_list.append(bi['_id'])
        #     else:
        #         print bi
        # print len(phn_list)
        # print len(p_list)
        # print len(p2_list)
    def get_lbs(self):
        c = self.get_mg_conn()
        in_file = '/Users/ufenqi/Documents/dataming' \
                  '/phn_risk/phn_analyze/order_jw'
        out_file = '/Users/ufenqi/Documents/dataming' \
                  '/phn_risk/phn_analyze/order_jw_1'
        wfp = open(out_file,'w')
        with open(in_file,'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                # print linelist[0]+'|8'
                cre=c.find({'_id': linelist[0]+'|8'})
                try:
                    cre = list(cre)
                    # print cre
                    lat1=float(linelist[-1])
                    lng1 = float(linelist[-2])
                    lat2=float(cre[0]['location']['coordinates'][1])
                    lng2=float(cre[0]['location']['coordinates'][0])
                    linelist.append(str(cre[0]['location']['coordinates'][0]))
                    linelist.append(str(cre[0]['location']['coordinates'][1]))
                    dtance=self.Distance2(lat1, lng1, lat2, lng2)
                    linelist.append(str(dtance))
                    wfp.write(','.join(linelist) + '\n')

                except:
                    pass

    def get_lbs_only(self):
        c = self.get_mg_conn()
        in_file = '/Users/ufenqi/Documents/dataming' \
                  '/phn_risk/phn_analyze/chain_chain_lease'
        out_file = '/Users/ufenqi/Documents/dataming' \
                   '/phn_risk/phn_analyze/chain_chain_lease_1'
        wfp = open(out_file, 'w')
        with open(in_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                # print linelist[0]+'|8'
                cre = c.find({'_id': linelist[1] + '|5'})
                try:
                    cre = list(cre)
                    # print cre
                    linelist.append(str(cre[0]['location']['coordinates'][0]))
                    linelist.append(str(cre[0]['location']['coordinates'][1]))
                    wfp.write(','.join(linelist) + '\n')

                except:
                    pass
    def get_distance(self):
        in_file = '/Users/ufenqi/Documents/dataming' \
                  '/phn_risk/phn_analyze/order_jw_all'
        out_file = '/Users/ufenqi/Documents/dataming' \
                   '/phn_risk/phn_analyze/order_jw_all_1'
        wfp = open(out_file, 'w')
        with open(in_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                # print linelist[0]+'|8'
                lat1 = float(linelist[7])
                lng1 = float(linelist[6])
                lat2 = float(linelist[5])
                lng2 = float(linelist[4])
                lat3 = float(linelist[-1])
                lng3 = float(linelist[-2])
                dtance_1_3 = self.Distance2(lat1, lng1, lat3, lng3)
                dtance_2_3 = self.Distance2(lat3, lng3, lat2, lng2)
                linelist.append(str(dtance_1_3))
                linelist.append(str(dtance_2_3))
                wfp.write(','.join(linelist) + '\n')
    def Distance2(self, lat1, lng1, lat2, lng2):  # 经纬度距离计算方法
        radlat1 = radians(lat1)
        radlat2 = radians(lat2)
        a = radlat1 - radlat2
        b = radians(lng1) - radians(lng2)
        s = 2 * asin(sqrt(pow(sin(a / 2), 2) + cos(radlat1) * cos(radlat2) * pow(sin(b / 2), 2)))
        earth_radius = 6378.137
        s = s * earth_radius
        if s < 0:
            return -s
        else:
            return s




if __name__ == '__main__':
    pd = PutData()
    # pd.mongodbselect()
    # pd.test()
    pd.bagging()
    # pd.tongdun()
    # pd.overday_num()
    # pd.relation()
    # pd.get_lbs()
    # pd.get_lbs_only()
    # pd.get_distance()