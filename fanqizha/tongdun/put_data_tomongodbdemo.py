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





if __name__ == '__main__':
    pd = PutData()
    pd.mongodbselect()
    # pd.test()
