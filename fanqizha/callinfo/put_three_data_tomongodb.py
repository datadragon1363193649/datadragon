#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
将数据put进mongodb
"""

import os
import sys

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath=os.path.split(os.path.realpath(_abs_path))[0]
sys.path.append(apath)
# sys.path.append(apath+'/log')
import traceback
import json
import pymongo as pm
import time
import datetime
import config.offline_db_conf as dconf
import MySQLdb
import logging
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] '
                       '%(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=apath+'/log/'+dconf.log_filename,
                filemode='a')
_debug = False


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
        self.mysqlhost=dconf.mysql_host
        self.mysqlport=dconf.mysql_port
        self.mysqluser=dconf.mysql_user
        self.mysqlpasswd=dconf.mysql_passwd
        self.mysqldb=dconf.mysql_db
        self.mysqlconn=None
        self.init_mysql_conn()

    def init_mysql_conn(self):
        self.mysqlconn= MySQLdb.connect(host=self.mysqlhost, port=self.mysqlport,
                                        user=self.mysqluser, passwd=self.mysqlpasswd,
                               db=self.mysqldb, charset="utf8")
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

    def put_data_tag(self, mysqlcursor,sql, tag):

        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def p(b):
            re = c.find({'_id': {'$in': b}}, {'somekey': 1})
            rsl = list(re)
            # print len(rsl)
            rerel = crel.find({'_id': {'$in': b}}, {'somekey': 1})
            rslrel = list(rerel)
            # 如果没有somekey 只返回_id, 有的话 返回这个字段和_id;
            # 用来判断该手机号存在与否, 如果不加这个条件会返回整个文档,
            # 增加网络传输时间;
            tu = []  # to return
            tirel = []  # to insert
            turel = []  # to update
            tis = set()
            res = set()
            for i in rsl:
                res.add(i['_id'])
            for i in b:
                if i in res:
                    tu.append(i)
            for i in b:
                if i in rslrel:
                    turel.append(i)
                else:
                    tis.add(i)
            for i in tis:
                tirel.append({'_id': i, tag: True})
            try:
                if turel:
                    crel.update_many({'_id': {'$in': turel}}, {'$set': {tag: True}})
                if tirel:
                    crel.insert_many(tirel)
                # print len(tu)
                # if tag == 'is_pass':
                #     self.put_data_relation(tu, 'contacted_pass_user')
                # if tag=='is_refuse':
                #     self.put_data_relation(tu, 'contacted_refuse_user')
            except Exception, e:
                # print str(e)
                pass
            # print len(tu)
            return tu
        try:
            self.mysqlconn.ping()
        except:
            self.init_mysql_conn()
            mysqlcursor = self.mysqlconn.cursor()
        n = mysqlcursor.execute(sql)
        logging.info('The number of %s is :%s',tag,n)
        bphn = []
        if n != 0:
            phncontent = mysqlcursor.fetchall()
            # print len(phncontent)
            # self.put_data_tag(passcontent, 'is_pass')
            i = 0
            buffer = []
            for fli in phncontent:
                phn = fli[0]
                if len(phn) > 0:
                    if phn.count('*') * 1.0 / len(phn) > 0.5:
                        continue
                else:
                    continue
                # print phn
                buffer.append(phn)
                i += 1
                if i % 1000 == 0:
                    bphn+=p(buffer)
                    buffer = []
            if i % 1000 != 0:
                bphn += p(buffer)
            # print buffer[1]
        return bphn

    def put_data_tag_all(self):
        mysqlcursor = self.mysqlconn.cursor()
        blacksql='select content from risk_auth_black_list where ' \
                 'createDate<CURDATE() AND createDate>=CURDATE()-1 AND type=1'
        passsql = 'select userName from risk_auth_credit_task  where authType ' \
                  'like "basic%" AND status like "pass%" and auditDate<CURDATE()' \
                  ' AND auditDate>=CURDATE()-1'
        refusesql = 'select userName from risk_auth_credit_task  where authType' \
                    ' like "basic%" AND status like "refuse%" and auditDate<CURDATE()' \
                    ' AND auditDate>=CURDATE()-1'
        self.put_data_tag(mysqlcursor,blacksql, 'is_black')
        # print 'done'
        mphn=self.put_data_tag(mysqlcursor,passsql, 'is_pass')
        detaillen=self.put_data_relation(mphn,'contacted_pass_user')
        logging.info('pass主联系人人数: %s ; %s 的总人数是: %s', len(mphn),
                     'contacted_pass_user', detaillen)
        # print 'done'
        mphn=self.put_data_tag(mysqlcursor,refusesql, 'is_refuse')
        detaillen=self.put_data_relation(mphn, 'contacted_refuse_user')
        logging.info('refuse主联系人人数: %s ; %s 的总人数是: %s', len(mphn),
                     'contacted_refuse_user', detaillen)
        # print 'done'
        # bpath = _abs_path + '/data/black'
        # rpath = _abs_path + '/data/refuse'
        # ppath = _abs_path + '/data/pass'

        # self.put_data_tag(rpath, 'is_refuse')
        # self.put_data_tag(ppath, 'is_pass')
    def put_black_data(self):
        mysqlcursor = self.mysqlconn.cursor()
        blacksql='select content from risk_auth_black_list where ' \
                 'createDate<CURDATE() AND createDate>=CURDATE()-1 AND type=1'
        self.put_data_tag(mysqlcursor,blacksql, 'is_black')

    def put_importance_other_data(self):
        mysqlcursor = self.mysqlconn.cursor()
        mobilesql = 'SELECT ru.mobile,racp.contactMobile1,racp.contactMobile2 ' \
                    'from risk.risk_credit_chain rcc ' \
                    'join risk.risk_user ru on rcc.userId=ru.id and ru.appCode="100001" ' \
                    'join risk.risk_auth_contact_person racp on rcc.userId=racp.userId ' \
                    'where rcc.pattern like "payday_basic%" and rcc.createDate BETWEEN "2017-05-01" and "2017-07-01"'
        self.put_importance_tag(mysqlcursor, mobilesql)
    def put_importance_tag(self,mobilesql):
        aaa=1
    def put_data_relation(self, fphn, tag):
        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def pdetail(b, t):
            re = crel.find({'_id': {'$in': b}}, {'somekey': 1})
            ti = []  # to insert
            tu = []  # to update
            res = set()
            for i in re:
                res.add(i['_id'])
            b = set(b)
            if t in b:
                b.remove(t)  # 从联系人中去除自己
            for i in b:
                if i in res:
                    tu.append(i)
                else:
                    ti.append({'_id': i, tag: [t], 'contacted_user': [t]})
            try:
                if tu:
                    crel.update_many({'_id': {'$in': tu}}, {'$addToSet': {tag: t,
                                                            'contacted_user': t}})
                if ti:
                    crel.insert_many(ti)
            except Exception, e:
                # print str(e)
                # traceback.print_exc()
                # print t
                pass
        def fdetail(phn):
            re = c.find({'_id':phn})
            rsl = list(re)
            # print re[0]['call_details'][0]
            phndetail=[]
            if 'call_details' in rsl[0]:
                # print fphn
                if 'call_info' in rsl[0]['call_details']:
                    # print rsl[0]['call_details'][0]
                    pcall_info=rsl[0]['call_details']['call_info']
                    for call in pcall_info:
                        if 'phone_num' in call:
                            p=call['phone_num']
                            p = p.strip()
                            p = p.replace(' ', '').replace('+86', '').replace('-', '').\
                                replace('*86', '').replace(';', '')
                            if len(p) == 13 and p[:2] == '86':
                                p = p[2:]
                            # print p
                            if len(p)>5:
                                phndetail.append(p.encode('utf-8'))
                                # print 'dayu',p
                    # print len(phndetail)
                    pdetail(phndetail,phn)
            return len(phndetail)
        i = 0
        buffer = {}
        phndetaillen=0
        for phn in fphn:
            i += 1
            if i % 100 == 0:
                # print "Start : %s" % time.ctime()
                time.sleep(1)
                # print "End : %s" % time.ctime()
            # print phn
            phndetaillen+=fdetail(phn)
        # print 'detail done'
        return phndetaillen
    # def put_data_relation_all(self):
    #     pass_contacted_fp = _abs_path + '/data/pass_contacts'
    #     refuse_contacted_fp = _abs_path + '/data/refuse_contacts'
    #     self.put_data_relation(pass_contacted_fp, 'contacted_pass_user')
    #     self.put_data_relation(refuse_contacted_fp, 'contacted_refuse_user')


if __name__ == '__main__':
    pd = PutData()
    pd.put_data_tag_all()
    # print apath
    # print dconf
    # pd.put_data_relation_all()
    # phn=['13871325741']
    # pd.put_data_relation(phn,'contacted_pass_user')