#! /usr/bin/env python
# -*- encoding: utf-8 -*-


import os
import sys
import traceback
import json
import pymongo as pm
import datetime
import offline_db_conf as dconf
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
    def get_contact(self):
        cm = self.get_mg_conn()
        in_file = '/Users/ufenqi/Documents/dataming/phn_risk/phn_lay_dot/dataset/phn_yu'
        out_file = '/Users/ufenqi/Documents/dataming/phn_risk/phn_lay_dot/dataset/phn_con_1'
        wfp = open(out_file, 'w')
        phn_list = []
        ni = 0
        with open(in_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phn_list.append(linelist[0])
                ni+=1
                if ni % 10 == 0:
                    phn_list = ['14704566570']
                    re = cm.find({'_id': {'$in': phn_list}})
                    rel = list(re)
                    for ri in rel:
                        if 'contacts' in ri:
                            con_list = [ri['_id']]
                            for phn in ri['contacts']:
                                if 'phone' in phn:
                                    con_list +=phn['phone']
                            wfp.write(','.join(con_list)+'\n')
                    phn_list = []
                    print ni
                    break
        wfp.close()
        fp.close()
if __name__ == '__main__':
    pdata = PutData()
    pdata.get_contact()


