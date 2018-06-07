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
import config.offline_db_conf as dconf
import MySQLdb
import logging
import copy
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=dconf.log_path+dconf.log_filename,
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

        self.mysqlhost_credit=dconf.mysql_host_credit
        self.mysqlport_credit=dconf.mysql_port_credit
        self.mysqluser_credit=dconf.mysql_user_credit
        self.mysqlpasswd_credit=dconf.mysql_passwd_credit
        self.mysqldb_credit=dconf.mysql_db_credit
        self.mysqlconn_credit=None
        self.init_mysql_credit_conn()

        self.mysqlhost_collect = dconf.mysql_host_collect
        self.mysqlport_collect = dconf.mysql_port_collect
        self.mysqluser_collect = dconf.mysql_user_collect
        self.mysqlpasswd_collect = dconf.mysql_passwd_collect
        self.mysqldb_collect = dconf.mysql_db_collect
        self.mysqlconn_collect = None
        self.init_mysql_collect_conn()

    def init_mysql_credit_conn(self):
        self.mysqlconn_credit= MySQLdb.connect(host=self.mysqlhost_credit,
                                        port=self.mysqlport_credit, user=self.mysqluser_credit,
                                        passwd=self.mysqlpasswd_credit,
                               db=self.mysqldb_credit, charset="utf8")

    def init_mysql_collect_conn(self):
        self.mysqlconn_collect= MySQLdb.connect(host=self.mysqlhost_collect,
                                        port=self.mysqlport_collect, user=self.mysqluser_collect,
                                        passwd=self.mysqlpasswd_collect,
                               db=self.mysqldb_collect, charset="utf8")

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

    def put_data_tag(self, mysql_credit_cursor,mysql_collect_cursor,sql, tag):

        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def p(b):
            re = c.find({'_id': b})
            rsl = list(re)
            # print len(rsl)
            rerel = crel.find({'_id': b})
            rslrel = list(rerel)
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
            if b in res:
                tu.append(b)
            if b in resrel:
                turel.append(b)
            else:
                tis.add(b)
            for i in tis:
                tirel.append({'_id': i, tag: True})
            tu_list = []
            i=0
            for tui in turel:
                tu_list.append(tui)
                i += 1
                if i % 10 == 0:
                    try:
                        crel.update_many({'_id': {'$in': tu_list}}, {'$set': {tag: True}})
                        tu_list = []
                    except Exception, e:
                        tu_list = []
                        logging.info(tag,' update_many error', tu_list)
                        # traceback.print_exc()
                        # print t
                        pass
            try:
                crel.update_many({'_id': {'$in': tu_list}}, {'$set': {tag: True}})
            except Exception, e:
                logging.info(tag, ' update_many error', tu_list)
                # traceback.print_exc()
                # print t
                pass
            ti_list = []
            i=0
            for tii in tirel:
                ti_list.append(tii)
                i += 1
                if i % 10 == 0:
                    try:
                        crel.insert_many(ti_list)
                        ti_list = []
                    except Exception, e:
                        ti_list = []
                        logging.info(tag, ' insert_many error', ti_list)
                        # traceback.print_exc()
                        # print t
                        pass
            try:
                crel.insert_many(ti_list)
            except Exception, e:
                logging.info(tag, ' insert_many error', ti_list)
                # traceback.print_exc()
                # print t
                pass
            # try:
            #     if turel:
            #         crel.update_many({'_id': {'$in': turel}}, {'$set': {tag: True}})
            #     if tirel:
            #         crel.insert_many(tirel)
            #     # print len(tu)
            #     # if tag == 'is_pass':
            #     #     self.put_data_relation(tu, 'contacted_pass_user')
            #     # if tag=='is_refuse':
            #     #     self.put_data_relation(tu, 'contacted_refuse_user')
            # except Exception, e:
            #     # print str(e)
            #     pass
            # print len(tu)
            return tu
        try:
            self.mysqlconn_credit.ping()
        except:
            self.init_mysql_credit_conn()
            mysql_credit_cursor = self.mysqlconn_credit.cursor()
        n = mysql_credit_cursor.execute(sql)
        # logging.info('The number of %s is :%s',tag,n)
        # args = ','.join(['%s'] * len(n))
        # rum_sql = 'select rum.mobile from from ru_user_mobile rum ' \
        #                ' where rum.task_id in (%s)' % args
        bphn = []
        if n != 0:
            task_id_tup = mysql_credit_cursor.fetchall()
            task_id_list=[]
            for tid in task_id_tup:
                task_id_list.append(tid[0])
            args = ','.join(['%s'] * len(task_id_list))
            rum_sql = 'select mobile from ru_user_mobile ' \
                      ' where task_id in (%s)' % args
            try:
                self.mysqlconn_collect.ping()
            except:
                self.init_mysql_collect_conn()
                mysql_collect_cursor = self.mysqlconn_collect.cursor()

            cn = mysql_collect_cursor.execute(rum_sql,task_id_list)
            logging.info('The number of %s is :%s', tag, cn)

            if cn != 0:
                phncontent = mysql_collect_cursor.fetchall()
                i = 0

                for fli in phncontent:
                    # 判断带＊电话号
                    phn = fli[0]
                    if len(phn) > 0:
                        if phn.count('*') * 1.0 / len(phn) > 0.5:
                            continue
                    else:
                        continue
                    # print phn
                    bphn+=p(phn)
                    i += 1
                    if i % 100 == 0:
                        time.sleep(1)
                # print buffer[1]
        else:
            logging.info('The number of %s is :%s', tag, 0)
        return bphn

    def put_data_black(self, mysql_credit_cursor,sql, tag):

        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def p(b):
            re = c.find({'_id': b})
            rsl = list(re)
            # print len(rsl)
            rerel = crel.find({'_id': b})
            rslrel = list(rerel)
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
            if b in res:
                tu.append(b)

            if b in resrel:
                turel.append(b)
            else:
                tis.add(b)
            for i in tis:
                tirel.append({'_id': i, tag: True})
            tu_list=[]
            i=0
            for tui in turel:
                tu_list.append(tui)
                i+=1
                if i % 10 == 0:
                    try:
                        crel.update_many({'_id': {'$in': tu_list}}, {'$set': {tag: True}})
                        tu_list=[]
                    except Exception, e:
                        tu_list=[]
                        logging.info(tag,' update_many error',tu_list)
                        # traceback.print_exc()
                        # print t
                        pass
            try:
                crel.update_many({'_id': {'$in': tu_list}}, {'$set': {tag: True}})
            except Exception, e:
                logging.info(tag,' update_many error', tu_list)
                # traceback.print_exc()
                # print t
                pass
            ti_list=[]
            i=0
            for tii in tirel:
                ti_list.append(tii)
                i += 1
                if i % 10 == 0:
                    try:
                        crel.insert_many(ti_list)
                        ti_list = []
                    except Exception, e:
                        ti_list = []
                        logging.info(tag,' insert_many error', ti_list)
                        # traceback.print_exc()
                        # print t
                        pass
            try:
                crel.insert_many(ti_list)
            except Exception, e:
                logging.info(tag,' insert_many error', ti_list)
                # traceback.print_exc()
                # print t
                pass
            # try:
            #     if turel:
            #         crel.update_many({'_id': {'$in': turel}}, {'$set': {tag: True}})
            #     if tirel:
            #         crel.insert_many(tirel)
            #     # print len(tu)
            #     # if tag == 'is_pass':
            #     #     self.put_data_relation(tu, 'contacted_pass_user')
            #     # if tag=='is_refuse':
            #     #     self.put_data_relation(tu, 'contacted_refuse_user')
            # except Exception, e:
            #     # print str(e)
            #     pass
            # print len(tu)
        try:
            self.mysqlconn_credit.ping()
        except:
            self.init_mysql_credit_conn()
            mysql_credit_cursor = self.mysqlconn_credit.cursor()
        n = mysql_credit_cursor.execute(sql)
        logging.info('The number of %s is :%s',tag,n)
        bphn = []
        if n != 0:
            phncontent = mysql_credit_cursor.fetchall()
            i = 0
            for fli in phncontent:
                # 判断带＊电话号
                phn = fli[0]
                if len(phn) > 0:
                    if phn.count('*') * 1.0 / len(phn) > 0.5:
                        continue
                else:
                    continue
                # print phn
                p(phn)
                i += 1
                if i % 100 == 0:
                    time.sleep(1)

    def put_data_tag_authentication(self):
        mysql_credit_cursor = self.mysqlconn_credit.cursor()
        mysql_collect_cursor = self.mysqlconn_collect.cursor()
        black_sql='select content ' \
                 ' from rc_black_list ' \
                 ' where create_Date < CURDATE() AND ' \
                 ' create_Date>=DATE_ADD(CURDATE(),INTERVAL -1 DAY) AND type=1'
        pass_sql_cc='select task_id from rc_credit_chain ' \
                    'where audit_date >=DATE_ADD(CURDATE(),INTERVAL -1 DAY) ' \
                  ' and audit_date <CURDATE()' \
                    ' and pattern in("hsqb") and status >=30'
        refuse_sql_cc = 'select task_id from rc_credit_chain ' \
                    'where audit_date >=DATE_ADD(CURDATE(),INTERVAL -1 DAY) ' \
                  ' and audit_date <CURDATE()' \
                    ' and pattern in("hsqb") and status <30'

        self.put_data_black(mysql_credit_cursor,black_sql, 'is_black')
        # print 'done'
        mphn=self.put_data_tag(mysql_credit_cursor,mysql_collect_cursor,pass_sql_cc, 'is_pass')
        if mphn > 0:
            detaillen = self.put_data_relation(mphn, 'contacted_pass_user')
            logging.info('pass主联系人人数: %s ; %s 的总人数是: %s', len(mphn),
                         'contacted_pass_user', detaillen)
        else:
            logging.info('pass主联系人人数: %s ; %s 的总人数是: %s', 0,
                         'contacted_pass_user', 0)
        # print 'done'
        mphn=self.put_data_tag(mysql_credit_cursor,mysql_collect_cursor,refuse_sql_cc, 'is_refuse')
        if mphn > 0:
            detaillen = self.put_data_relation(mphn, 'contacted_refuse_user')
            logging.info('refuse主联系人人数: %s ; %s 的总人数是: %s',
                         len(mphn), 'contacted_refuse_user', detaillen)
        else:
            logging.info('refuse主联系人人数: %s ; %s 的总人数是: %s',
                         0, 'contacted_refuse_user', 0)

    def put_data_relation(self, fphn, tag):
        c = self.get_mg_conn()
        crel = self.get_mg_connrelation()
        def pdetail(b, t):
            re = crel.find({'_id': {'$in': b}})
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
            tu_list=[]
            i=0
            for tui in tu:
                tu_list.append(tui)
                i+=1
                if i % 10 == 0:
                    try:
                        crel.update_many({'_id': {'$in': tu_list}}, {'$addToSet': {tag: t, 'contacted_user': t}})
                        tu_list=[]
                    except Exception, e:
                        tu_list=[]
                        logging.info(tag, ' update_many error', tu_list)
                        # traceback.print_exc()
                        # print t
                        pass
            try:
                crel.update_many({'_id': {'$in': tu_list}}, {'$addToSet': {tag: t, 'contacted_user': t}})
            except Exception, e:
                logging.info(tag, ' update_many error', tu_list)
                # traceback.print_exc()
                # print t
                pass
            ti_list=[]
            i=0
            for tii in ti:
                ti_list.append(tii)
                i += 1
                if i % 10 == 0:
                    try:
                        crel.insert_many(ti_list)
                        ti_list = []
                    except Exception, e:
                        ti_list = []
                        logging.info(tag, ' insert_many error', ti_list)
                        # traceback.print_exc()
                        # print t
                        pass
            try:
                crel.insert_many(ti_list)
            except Exception, e:
                logging.info(tag, ' insert_many error', ti_list)
                # traceback.print_exc()
                # print t
                pass

        def fdetail(phn):
            re = c.find({'_id':phn})
            rsl = list(re)
            # print re[0]['call_details'][0]
            phndetail={}
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
                                phndetail.setdefault(p.encode('utf-8'),0)
                                # phndetail.append(p.encode('utf-8'))
                                # print 'dayu',p
                    # print len(phndetail)
                    phndetail=phndetail.keys()
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
                logging.info(tag, ' error',tu,ti )
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

    def put_importance_other_data(self):
        mysql_credit_cursor = self.mysqlconn_credit.cursor()
        mysql_collect_cursor = self.mysqlconn_collect.cursor()
        rcc_sql='select task_id from rc_credit_chain ' \
                    'where audit_date >=DATE_ADD(CURDATE(),INTERVAL -1 DAY) ' \
                  ' and audit_date <CURDATE() and pattern in("hsqb")'
        tag='contacted_important_user'
        try:
            self.mysqlconn_credit.ping()
        except:
            self.init_mysql_credit_conn()
            mysql_credit_cursor = self.mysqlconn_credit.cursor()
        rcc_n = mysql_credit_cursor.execute(rcc_sql)
        # n = mysqlcursor.execute(mobilesql)
        logging.info('The number of %s is :%s',tag,rcc_n)
        if rcc_n != 0:
            task_id_tulp = mysql_credit_cursor.fetchall()
            task_id_list=[]
            for tid in task_id_tulp:
                task_id_list.append(tid[0])

            args = ','.join(['%s'] * len(task_id_list))
            rum_sql = 'select task_id,mobile from ru_user_mobile ' \
                      ' where task_id in (%s)' % args
            try:
                self.mysqlconn_collect.ping()
            except:
                self.init_mysql_collect_conn()
                mysql_collect_cursor = self.mysqlconn_collect.cursor()

            rum_n = mysql_collect_cursor.execute(rum_sql, task_id_list)
            phn_dic={}
            if rum_n != 0:
                task_phn = mysql_collect_cursor.fetchall()
                for tp in task_phn:
                    phn_dic[tp[0]]=tp[1]

                rcp_sql = 'select task_id,contact_mobile1,contact_mobile2 from ru_contact_person ' \
                          ' where task_id in (%s)' % args
                try:
                    self.mysqlconn_collect.ping()
                except:
                    self.init_mysql_collect_conn()
                    mysql_collect_cursor = self.mysqlconn_collect.cursor()

                rcp_n= mysql_collect_cursor.execute(rcp_sql, task_id_list)
                if rcp_n != 0:
                    import_phn=mysql_collect_cursor.fetchall()
                    i=0
                    for imp in import_phn:
                        task_id = imp[0]
                        phn=phn_dic[task_id]
                        imphn=[]
                        if imp[1] is not None:
                            imphn.append(imp[1])
                        if imp[2] is not None:
                            imphn.append(imp[2])
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
                        crel.insert({'_id': i, tag: True,'yellowname':bufdic[i]})
                except Exception, e:
                    # print '111', str(e)
                    pass

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
    # 写入二度关系数据；黑名单、拒绝、通过三种标签
    pd.put_data_tag_authentication()

    # 一天importance_other数据插入mongodb接口
    pd.put_importance_other_data()

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
