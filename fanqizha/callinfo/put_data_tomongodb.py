#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
将数据put进mongodb

author: xiaoliang.liu at jiandanjiekuan dot com
date: 2017-02-28
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
import config.offline_db_conf as dconf
import MySQLdb
import logging
import copy
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
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
        self.mysqlconn= MySQLdb.connect(host=self.mysqlhost,
                                        port=self.mysqlport, user=self.mysqluser,
                                        passwd=self.mysqlpasswd,
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
            resrel=set()
            tis = set()
            res = set()
            for i in rsl:
                # print i['_id']
                res.add(i['_id'])
            for i in rslrel:
                resrel.add(i['_id'])
            for i in b:
                if i in res:
                    tu.append(i)
            for i in b:
                if i in resrel:
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
                # 判断带＊电话号
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
                    time.sleep(4)
                    buffer = []
            if i % 1000 != 0:
                bphn += p(buffer)
            # print buffer[1]
        return bphn


    def put_data_tag_authentication(self):
        mysqlcursor = self.mysqlconn.cursor()
        blacksql='select content from risk_auth_black_list where ' \
                 'createDate<CURDATE() AND createDate>=CURDATE()-1 AND type=1'
        passsql = 'select ru.mobile from risk.risk_credit_chain ' \
                  'cc join risk.risk_user ru on ru.id = cc.userId ' \
                  'where cc.auditDate >=CURDATE()-1 and cc.auditDate ' \
                  '<CURDATE() and cc.pattern = "payday_basic" ' \
                  'and cc.status REGEXP "^pass"'
        refusesql = 'select ru.mobile from risk.risk_credit_chain ' \
                    'cc join risk.risk_user ru on ru.id = cc.userId ' \
                  'where cc.auditDate >=CURDATE()-1 and cc.auditDate ' \
                    '<CURDATE() and cc.pattern = "payday_basic" ' \
                  'and cc.status REGEXP "^refuse"'
        self.put_data_tag(mysqlcursor,blacksql, 'is_black')
        # print 'done'
        mphn=self.put_data_tag(mysqlcursor,passsql, 'is_pass')
        detaillen=self.put_data_relation(mphn,'contacted_pass_user')
        logging.info('pass主联系人人数: %s ; %s 的总人数是: %s', len(mphn),
                     'contacted_pass_user', detaillen)
        # print 'done'
        mphn=self.put_data_tag(mysqlcursor,refusesql, 'is_refuse')
        detaillen=self.put_data_relation(mphn, 'contacted_refuse_user')
        logging.info('refuse主联系人人数: %s ; %s 的总人数是: %s',
                     len(mphn),'contacted_refuse_user', detaillen)

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
                    crel.update_many({'_id': {'$in': tu}}, {'$addToSet': {tag: t, 'contacted_user': t}})
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
                            p = p.replace(' ', '').replace('+86', '').replace('-', '').replace('*86', '').replace(';', '')
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
    def put_importance_tag(self,imphn,phn,tag):
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
                    ti.append({'_id': i, tag: [t]})
            try:
                if tu:
                    crel.update_many({'_id': {'$in': tu}}, {'$addToSet': {tag: t}})
                if ti:
                    crel.insert_many(ti)
            except Exception, e:
                # print str(e)
                # traceback.print_exc()
                # print t
                pass
        pdetail(imphn,phn)
    def put_importance_tag_batch(self,imphn,tag):
        crel = self.get_mg_connrelation()
        re = crel.find({'_id': {'$in': imphn.keys()}}, {'somekey': 1})
        ti = []  # to insert
        # tu = []  # to update
        res = set()
        for i in re:
            res.add(i['_id'])
        b = set(imphn.keys())
        # if t in b:
        #     b.remove(t)  # 从联系人中去除自己
        try:
            for i in b:
                if i in res:
                    # print imphn[i]
                    crel.update({'_id':i},{'$addToSet': {tag:{'$each': imphn[i]}}})
                else:
                    ti.append({'_id': i, tag: [imphn[i]]})
            if ti:
                crel.insert_many(ti)
        except Exception, e:
            # print str(e)
            # traceback.print_exc()
            # print t
            pass

    def put_black_data_batch(self):
        mysqlcursor = self.mysqlconn.cursor()
        blacksql='select content from risk_auth_black_list where type=1'
        self.put_data_tag(mysqlcursor,blacksql, 'is_black')
    def put_importance_other_data_batch(self):
        s_file=apath+'/data/importphone.txt'
        tag='contacted_important_user'
        with open(s_file,'r') as fp:
            i=0
            imphn = {}
            phn=[]
            for line in fp:
                linelist=line.strip().split(',')
                if linelist[1] is not None:
                    imphn.setdefault(linelist[1],[])
                    imphn[linelist[1]].append(linelist[0])
                    # print linelist[1],linelist[0]
                    # imphn.append(linelist[1])
                    # phn.append(linelist[0])
                if linelist[2] is not None:
                    imphn.setdefault(linelist[2], [])
                    imphn[linelist[2]].append(linelist[0])
                    # print linelist[2], linelist[0]
                if len(imphn)<1:
                    continue
                i += 1
                if i % 1000 == 0:
                    self.put_importance_tag_batch(imphn, tag)
                    time.sleep(4)
                    # break
                    imphn={}
            if i % 1000 != 0:
                self.put_importance_tag_batch(imphn, tag)
            # self.put_importance_tag_batch(imphn, tag)
                    # phn=[]
                # print imphn
                # break
    def put_importance_other_data(self):
        mysqlcursor = self.mysqlconn.cursor()
        mobilesql = 'SELECT ru.mobile,racp.contactMobile1,racp.contactMobile2 ' \
                    'from risk.risk_credit_chain rcc ' \
                    'join risk.risk_user ru on rcc.userId=ru.id ' \
                    'join risk.risk_auth_contact_person racp on rcc.userId=racp.userId ' \
                    'where rcc.pattern like "payday_basic%" and ' \
                    'rcc.createDate >=CURDATE()-1 and rcc.createDate <CURDATE()'
        tag='contacted_important_user'
        try:
            self.mysqlconn.ping()
        except:
            self.init_mysql_conn()
            mysqlcursor = self.mysqlconn.cursor()
        n = mysqlcursor.execute(mobilesql)
        logging.info('The number of %s is :%s',tag,n)
        if n != 0:
            phncontent = mysqlcursor.fetchall()
            i=0
            for fli in phncontent:
                phn = fli[0]
                imphn=[]
                if fli[1] is not None:
                    imphn.append(fli[1])
                if fli[2] is not None:
                    imphn.append(fli[2])
                if len(imphn)<1:
                    continue
                i += 1
                if i % 100 == 0:
                    time.sleep(1)
                self.put_importance_tag(imphn,phn,tag)
    def put_yellow_data_tag(self, phonelist,namelist, tag):

        # c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def p(bufdic):
            phone=bufdic.keys()
            rerel = crel.find({'_id': {'$in': phone}}, {'somekey': 1})
            rslrel = list(rerel)
            # print rslrel
            tu = []  # to return
            tirel = []  # to insert
            turel = []  # to update
            namelist=[]
            tis = set()
            res = set()
            for i in rslrel:
                res.add(i['_id'])
            # print len(res)

            for i in phone:
                try:
                    if i in res:
                        crel.update({'_id':i}, {'$set': {tag: True, 'yellowname': bufdic[i]}})
                        turel.append(i)
                        namelist.append(bufdic[i])
                    else:
                        tis.add(i)
                        # print tis
                        # print 'qweq',i
                        crel.insert({'_id': i, tag: True,'yellowname':bufdic[i]})
                except Exception, e:
                    # print '111', str(e)
                    pass

            # print tis
            # print len(turel)
            # for i in tis:
            #     tirel.append({'_id': i, tag: True,'yellowname':bufdic[i]})
            # if tirel:
            #     crel.insert_many(tirel)

            return tu
        i = 0
        bufferpho = {}
        buname=[]
        # print phonelist[:5]
        for i in range(0,len(phonelist)):
            # print phn
            bufferpho[phonelist[i]]=namelist[i]
            i += 1
            if i % 1000 == 0:
                # print bufferpho
                p(bufferpho)
                # print 'endp'
                bufferpho = {}
                # break
        if i % 1000 != 0:
            p(bufferpho)
        # print buffer[1]

    def put_data_tag_yellow(self):

        bank_file=apath+'/data/bank_yellowpage'
        collection_file = apath + '/data/collection_yellowpage'
        bankmobile=[]
        bankname=[]
        with open(bank_file,'r') as fp:
            for line in fp :
                linelist=line.strip().split(',')
                # print linelist
                bankname.append(linelist[0])
                bankmobile.append(linelist[1])
        cuishoumobile=[]
        cuishouname=[]
        with open(collection_file,'r') as fp:
            for line in fp :
                linelist=line.strip().split(',')
                # print linelist
                cuishouname.append(linelist[0])
                cuishoumobile.append(linelist[1])
        # print len(cuishoumobile)
        # bankyellow = pda.read_excel(apath+'/data/bank_yellowpage.xlsx')
        # cuishouyellowdf = pda.read_excel(apath+'/data/collection_yellowpage.xlsx')
        # bankyellowdf.columns = ['source', 'phone']
        # cuishouyellowdf.columns = ['source', 'phone']
        # bankmobile = bankyellowdf['phone'].values.tolist()
        # cuishoumobile = cuishouyellowdf['phone'].values.tolist()
        # bankname = bankyellowdf['source'].values.tolist()
        # cuishouname = cuishouyellowdf['source'].values.tolist()
        self.put_yellow_data_tag(bankmobile, bankname, 'is_bank_yellowpage')
        self.put_yellow_data_tag(cuishoumobile, cuishouname, 'is_collection_yellowpage')
    def put_data_tag_batch(self, phn, tag):

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
            resrel=set()
            tis = set()
            res = set()
            for i in rsl:
                res.add(i['_id'])
            for i in rslrel:
                resrel.add(i['_id'])
            for i in b:
                if i in res:
                    tu.append(i)
            for i in b:
                if i in resrel:
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
        bphn = []
        bphn += p(phn)
            # print buffer[1]
        # return bphn
    def put_refuse_data_batch(self):
        s_file = apath + '/data/isrefuse_10.txt'
        tag = 'is_refuse'
        with open(s_file, 'r') as fp:
            i = 0
            phn = []
            for line in fp:
                linelist = line.strip().split(',')
                phn.append(linelist[0])
                i += 1
                if i % 1000 == 0:
                    self.put_data_tag_batch(phn,tag)
                    time.sleep(4)
                    # break
                    phn = []
            if i % 1000 != 0:
                self.put_data_tag_batch(phn, tag)
    def put_pass_data_batch(self):
        s_file = apath + '/data/ispass_10.txt'
        tag = 'is_pass'
        with open(s_file, 'r') as fp:
            i = 0
            phn = []
            for line in fp:
                linelist = line.strip().split(',')
                phn.append(linelist[0])
                i += 1
                if i % 1000 == 0:
                    self.put_data_tag_batch(phn,tag)
                    time.sleep(4)
                    # break
                    phn = []
            if i % 1000 != 0:
                self.put_data_tag_batch(phn, tag)
if __name__ == '__main__':
    pd = PutData()
    pd.put_data_tag_authentication()

    # 一天importance_other数据插入mongodb接口
    pd .put_importance_other_data()

    # 一批black数据插入mongodb接口
    # pd.put_black_data_batch()
    # 一批refuse数据插入mongodb接口
    # pd.put_refuse_data_batch()
    # 一批pass数据插入mongodb接口
    # pd.put_pass_data_batch()

    # 一批importance_other数据插入mongodb接口
    # pd.put_importance_other_data_batch()
    # 一批黄页数据插入mongodb接口
    # pd.put_data_tag_yellow()






    # print apath
    # print dconf
    # pd.put_data_relation_all()
    # phn=['13871325741']
    # pd.put_data_relation(phn,'contacted_pass_user')