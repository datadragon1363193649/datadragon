# -*- encoding: utf-8 -*-
import os
import sys
import numpy as np
import pandas as pd
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath = os.path.split(os.path.realpath(_abs_path))[0]
sys.path.append(apath)
# sys.path.append(apath+'/log')
import traceback
import json
import pymongo as pm
import time
import datetime
import db_conf_501 as dconf
import logging
import copy
import MySQLdb as mqdb
import sql_file as sqlfile
import code_lst as codelst
import collections
import time


class Rds_Data():
    def __init__(self):
        # self.mhost1 = dconf.mg_host1
        # 初始化mongo连接
        self.mhost2 = dconf.mg_host2
        self.mreplicat_set = dconf.mg_replicat_set
        self.mgdb = dconf.mg_db
        self.mgcollection = dconf.mg_collection  # call_info
        self.mgcollectionrelation = dconf.mg_coll_relation  # call_relation
        self.mgcollection_tongdun = dconf.mg_coll_tongdun  # risk_user_tongdun
        self.mgcollectionrelation_tongdun_detail = dconf.mg_coll_tongdun_detail  # risk_user_tongdun_detail
        self.mgcollection_union = dconf.mg_collection_detail  # risk_union_detail
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.mgconn = None
        self.init_mg_conn()

        # 初始化mysql连接
        self.mqhost = dconf.dbInfo[0]
        self.mquser = dconf.dbInfo[1]
        self.mqpwd = dconf.dbInfo[2]
        self.mqport = dconf.dbInfo[3]
        self.mqdbinfo = dconf.dbInfo[4]
        self.mqchrset = dconf.dbInfo[5]
        self.mqconn = None

        # 初始化sql
        # 审批结果
        self.audit_result_1 = sqlfile.audit_result_1
        self.audit_result_2 = sqlfile.audit_result_2

        # 还款信息
        self.repay_info_1 = sqlfile.repay_info_1
        self.repay_info_2 = sqlfile.repay_info_2
        self.repay_info_3 = sqlfile.repay_info_3
        self.repay_info_4 = sqlfile.repay_info_4

        # 借款信息
        self.loan_info_1 = sqlfile.loan_info_1
        self.loan_info_2 = sqlfile.loan_info_2
        self.loan_info_3 = sqlfile.loan_info_3
        self.loan_info_4 = sqlfile.loan_info_4

        # 客户基本信息
        self.cust_info_1 = sqlfile.cust_info_1
        self.cust_info_2 = sqlfile.cust_info_2

        # 评分类
        self.score_info_1 = sqlfile.score_info_1
        self.score_info_2 = sqlfile.score_info_2

        # 闪银借贷行为
        self.wecash_behavior_1 = sqlfile.wecash_behavior_1
        self.wecash_behavior_2 = sqlfile.wecash_behavior_2

        # 银联
        self.wecash_union_1 = sqlfile.wecash_union_1
        self.wecash_union_2 = sqlfile.wecash_union_2

        self.wecash_union2_1 = sqlfile.wecash_union2_1
        self.wecash_union2_2 = sqlfile.wecash_union2_2

        # 同盾
        self.tongdun_info_1 = sqlfile.tongdun_info_1
        self.tongdun_info_2 = sqlfile.tongdun_info_2

        # 初始化列表
        # 银联
        self.wecashunion_lst = codelst.wecashunion_lst

        # 同盾
        self.tongdun_lst = codelst.tongdun_lst
        self.tongdun_lst_1 = codelst.tongdun_lst_1

        # 导出文件
        self.auditresult_txt = '/home/yanghan/risk_dataSet/data/RDS_auditresult_20171031.txt'
        self.repayinfo_txt = '/home/yanghan/risk_dataSet/data/RDS_repayinfo_20171031.txt'
        self.loaninfo_txt = '/home/yanghan/risk_dataSet/data/RDS_loaninfo_20171031.txt'
        self.custinfo_txt = '/home/yanghan/risk_dataSet/data/RDS_custinfo_20171031.txt'
        self.scoreinfo_txt = '/home/yanghan/risk_dataSet/data/RDS_scoreinfo_20171031.txt'
        self.wecashbehavior_txt = '/home/yanghan/risk_dataSet/data/RDS_wecashbehavior_20171031.txt'
        self.unioninfo_txt = '/home/yanghan/risk_dataSet/data/RDS_unioninfo_20171031.txt'
        self.tongduninfo_txt = '/home/yanghan/risk_dataSet/data/RDS_tongduninfo_20171031.txt'

    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost2], maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)

    def get_mq_conn(self):
        self.mqconn = mqdb.connect(host=self.mqhost, user=self.mquser, passwd=self.mqpwd, port=self.mqport,
                                   db=self.mqdbinfo, charset=self.mqchrset)
        return self.mqconn

    def get_mg_conn(self, mc):
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]

    # 审批结果
    def rds_audit(self):
        audb = self.get_mq_conn()
        try:
            alluidframe = pd.read_sql(self.audit_result_1, audb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        istart = 0
        igap = 100000
        uidtmp = ()
        while istart < len(alluid):
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.audit_result_2_tmp = self.audit_result_2.format(user_id=uidtmp)
            auframe = pd.read_sql(self.audit_result_2_tmp, audb)
            auframe.to_csv(self.auditresult_txt, sep='^', mode='a', index=False, header=False)
        audb.close()

    # 还款信息
    def rds_repay(self):
        redb = self.get_mq_conn()
        try:
            alluidframe = pd.read_sql(self.repay_info_1, redb)
            alluidframe1 = pd.read_sql(self.repay_info_3, redb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        alluid1 = tuple(alluidframe1.userid)
        istart = 0
        igap = 100000
        uidtmp = ()
        while istart < len(alluid):
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.repay_info_2_tmp = self.repay_info_2.format(user_id=uidtmp)
            reframe = pd.read_sql(self.repay_info_2_tmp, redb)
            reframe.to_csv(self.repayinfo_txt, sep='^', mode='a', index=False, header=False)
        istart = 0
        uidtmp = ()
        while istart < len(alluid1):
            uidtmp = alluid1[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            uidtmp = tuple([str(i) for i in uidtmp])
            self.repay_info_4_tmp = self.repay_info_4.format(user_id=uidtmp)
            reframe1 = pd.read_sql(self.repay_info_4_tmp, redb)
            reframe1.to_csv(self.repayinfo_txt, sep='^', mode='a', index=False, header=False)
        redb.close()

    # 借款信息
    def rds_loan(self):
        lodb = self.get_mq_conn()
        try:
            alluidframe = pd.read_sql(self.loan_info_1, lodb)
            alluidframe1 = pd.read_sql(self.loan_info_3, lodb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        alluid1 = tuple(alluidframe1.userid)
        istart = 0
        igap = 100000
        uidtmp = ()
        while istart < len(alluid):
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.loan_info_2_tmp = self.loan_info_2.format(user_id=uidtmp)
            loframe = pd.read_sql(self.loan_info_2_tmp, lodb)
            loframe.to_csv(self.loaninfo_txt, sep='^', mode='a', index=False, header=False)
        istart = 0
        uidtmp = ()
        while istart < len(alluid1):
            uidtmp = alluid1[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            uidtmp = tuple([str(i) for i in uidtmp])
            self.loan_info_4_tmp = self.loan_info_4.format(user_id=uidtmp)
            loframe1 = pd.read_sql(self.loan_info_4_tmp, lodb)
            loframe1.to_csv(self.loaninfo_txt, sep='^', mode='a', index=False, header=False)
        lodb.close()

    # 客户基本信息
    def rds_custom(self):
        qmdb = self.get_mq_conn()
        qmgdb = self.get_mg_conn(self.mgcollection_tongdun)
        try:
            alluidframe = pd.read_sql(self.cust_info_1, qmdb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        istart = 0
        igap = 10000
        uidtmp = ()
        while istart < len(alluid):
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.cust_info_2_tmp = self.cust_info_2.format(user_id=uidtmp)
            qmframe = pd.read_sql(self.cust_info_2_tmp, qmdb)
            mgdic = collections.OrderedDict()
            mgdic.clear()
            mongokeylst = list(qmframe.mongokey)
            mongokeylst = [str(i) for i in mongokeylst]
            modocs = qmgdb.find({'_id': {'$in': mongokeylst}},
                                {'_id': 1, 'mobileAddressProvince': 1, 'mobileAddressCity': 1, 'idCardProvince': 1,
                                 'idCardCity': 1, 'idCardCounty': 1, 'eventId': 1})
            mdkeylst = ['idCardProvince', 'idCardCity', 'idCardCounty', 'mobileAddressProvince', 'mobileAddressCity',
                        'eventId']
            for md in modocs:
                if 'mongokey' in mgdic:
                    mgdic['mongokey'].append(md['_id'])
                else:
                    mgdic['mongokey'] = [md['_id']]
                for mdkey in mdkeylst:
                    if mdkey in md:
                        if mdkey in mgdic:
                            mgdic[mdkey].append(md[mdkey])
                        else:
                            mgdic[mdkey] = [md[mdkey]]
                    else:
                        if mdkey in mgdic:
                            mgdic[mdkey].append('')
                        else:
                            mgdic[mdkey] = ['']
            mgframe = pd.DataFrame(mgdic)
            mgframe['mongokey'] = mgframe['mongokey'].astype('str')
            qmframe['mongokey'] = qmframe['mongokey'].astype('str')
            qmframe = qmframe.merge(mgframe, left_on='mongokey', right_on='mongokey', how='left')
            qmframe.iloc[:, 1:].to_csv(self.custinfo_txt, sep='^', mode='a', index=False, header=False)
        qmdb.close()

    # 评分类
    def rds_score(self):
        qmdb = self.get_mq_conn()
        qmgdb = self.get_mg_conn(self.mgcollection_tongdun)
        try:
            alluidframe = pd.read_sql(self.score_info_1, qmdb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        istart = 0
        igap = 10000
        uidtmp = ()
        while istart < len(alluid):
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.score_info_2_tmp = self.score_info_2.format(user_id=uidtmp)
            qmframe = pd.read_sql(self.score_info_2_tmp, qmdb)
            mgdic = collections.OrderedDict()
            mgdic.clear()
            mongokeylst = list(qmframe.mongokey)
            mongokeylst = [str(i) for i in mongokeylst]
            modocs = qmgdb.find({'_id': {'$in': mongokeylst}}, {'_id': 1, 'finalScore': 1})
            mdkeylst = ['finalScore']
            for md in modocs:
                if 'mongokey' in mgdic:
                    mgdic['mongokey'].append(md['_id'])
                else:
                    mgdic['mongokey'] = [md['_id']]
                for mdkey in mdkeylst:
                    if mdkey in md:
                        if mdkey in mgdic:
                            mgdic[mdkey].append(md[mdkey])
                        else:
                            mgdic[mdkey] = [md[mdkey]]
                    else:
                        if mdkey in mgdic:
                            mgdic[mdkey].append('')
                        else:
                            mgdic[mdkey] = ['']
            mgframe = pd.DataFrame(mgdic)
            mgframe['mongokey'] = mgframe['mongokey'].astype('str')
            qmframe['mongokey'] = qmframe['mongokey'].astype('str')
            qmframe = qmframe.merge(mgframe, left_on='mongokey', right_on='mongokey', how='left')
            qmframe.iloc[:, 1:].to_csv(self.scoreinfo_txt, sep='^', mode='a', index=False, header=False)
        qmdb.close()

    # 闪银借贷行为
    def rds_wecashbehavior(self):
        qmdb = self.get_mq_conn()
        try:
            alluidframe = pd.read_sql(self.wecash_behavior_1, qmdb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        istart = 0
        igap = 10000
        uidtmp = ()
        while istart < len(alluid):
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.wecash_behavior_2_tmp = self.wecash_behavior_2.format(user_id=uidtmp)
            qmframe = pd.read_sql(self.wecash_behavior_2_tmp, qmdb)
            qmframe.to_csv(self.wecashbehavior_txt, sep='^', mode='a', index=False, header=False)
        qmdb.close()

    # 银联_8月后
    def rds_unioninfo_2(self):
        qmdb = self.get_mq_conn()
        qmgdb = self.get_mg_conn(self.mgcollection_union)
        try:
            alluidframe = pd.read_sql(self.wecash_union2_1, qmdb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        istart = 0
        igap = 10000
        uidtmp = ()
        mgdic = collections.OrderedDict()
        while istart < len(alluid):
            mgdic.clear()
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.wecash_union2_2_tmp = self.wecash_union2_2.format(user_id=uidtmp)
            qmframe = pd.read_sql(self.wecash_union2_2_tmp, qmdb)
            uidtmp = [str(i) for i in uidtmp]
            coldoc = qmgdb.find({'_id': {'$in': uidtmp}}, {'_id': 1, 'cardsStr': 1})
            for crel in coldoc:
                bankcardlst = []
                if crel and crel['cardsStr']:
                    bankjson = json.loads(crel['cardsStr'])
                    bankcardlst = bankjson.keys()
                    for bc in bankcardlst:
                        if bankjson[bc]['resultDetail']:
                            if 'userid' in mgdic:
                                mgdic['userid'].append(crel['_id'])
                            else:
                                mgdic['userid'] = [crel['_id']]
                            if 'cardnumber' in mgdic:
                                mgdic['cardnumber'].append(bc)
                            else:
                                mgdic['cardnumber'] = [bc]

                            rejson = json.loads(bankjson[bc]['resultDetail'])
                            for cdl in self.wecashunion_lst:
                                if cdl in rejson:
                                    if cdl in mgdic:
                                        mgdic[cdl].append(rejson[cdl])
                                    else:
                                        mgdic[cdl] = [rejson[cdl]]
                                else:
                                    if cdl in mgdic:
                                        mgdic[cdl].append('')
                                    else:
                                        mgdic[cdl] = ['']
                        else:
                            continue
                else:
                    continue
            qmframe['userid'] = qmframe['userid'].astype('str')
            qmframe['cardnumber'] = qmframe['cardnumber'].astype('str')
            mgframe = pd.DataFrame(mgdic)
            mgframe['userid'] = mgframe['userid'].astype('str')
            mgframe['cardnumber'] = mgframe['cardnumber'].astype('str')
            # print self.wecash_union2_2_tmp
            qmframe = qmframe.merge(mgframe, left_on=['userid', 'cardnumber'], right_on=['userid', 'cardnumber'],
                                    left_index=True, how='left')
            qmframe.iloc[:, 2:].to_csv(self.unioninfo_txt, sep='^', mode='a', index=False, header=False)

    # 同盾
    def rds_tongdun(self):
        qmdb = self.get_mq_conn()
        qmgdb = self.get_mg_conn(self.mgcollectionrelation_tongdun_detail)
        try:
            alluidframe = pd.read_sql(self.tongdun_info_1, qmdb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        istart = 0
        igap = 10000
        uidtmp = ()
        mgdic = collections.OrderedDict()
        while istart < len(alluid):
            mgdic.clear()
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.tongdun_info_2_tmp = self.tongdun_info_2.format(user_id=uidtmp)
            qmframe = pd.read_sql(self.tongdun_info_2_tmp, qmdb)
            uidtmp = [str(i) for i in uidtmp]
            coldoctd = qmgdb.find({'_id': {'$in': uidtmp}}, {'_id': 1, 'viewDetail': 1})
            for creltd in coldoctd:
                if creltd and creltd['viewDetail']:
                    tdjson = json.loads(creltd['viewDetail'])
                    if 'rules' in tdjson[0]:
                        # print tdjson[0]['rules'][0]['rule_detail']
                        if u'P2P网贷' in tdjson[0]['rules'][0]['rule_detail']:
                            print tdjson[0]['rules'][0]['rule_detail']
                        if 'userid' in mgdic:
                            mgdic['userid'].append(creltd['_id'])
                        else:
                            mgdic['userid'] = [creltd['_id']]
                        for tdcode in self.tongdun_lst:
                            if tdcode in self.tongdun_lst_1:
                                num_loan = 0
                                itmp = 0
                                num_tmp = ''
                                if tdjson[0]['rules']:
                                    for tdv in tdjson[0]['rules']:
                                        if 'rule_detail' in tdv and 'rule_name' in tdv:
                                            rude = tdv['rule_detail']
                                            runa = tdv['rule_name']
                                            if runa == tdcode:
                                                for i in rude:
                                                    ilst = i.split(':')
                                                    itmp += int(ilst[1])
                                                num_tmp = itmp
                                            else:
                                                continue
                                        else:
                                            continue
                                    if tdcode in mgdic:
                                        mgdic[tdcode].append(num_tmp)
                                    else:
                                        mgdic[tdcode] = [num_tmp]
                                else:
                                    if tdcode in mgdic:
                                        mgdic[tdcode].append('')
                                    else:
                                        mgdic[tdcode] = ['']
                            else:
                                rude = ''
                                if tdjson[0]['rules']:
                                    for tdv in tdjson[0]['rules']:
                                        if 'rule_detail' in tdv and 'rule_name' in tdv:
                                            runa = tdv['rule_name']
                                            if runa == tdcode:
                                                rude = tdv['rule_detail']
                                            else:
                                                continue
                                    if tdcode in mgdic:
                                        mgdic[tdcode].append(rude)
                                    else:
                                        mgdic[tdcode] = [rude]
                                else:
                                    if tdcode in mgdic:
                                        mgdic[tdcode].append('')
                                    else:
                                        mgdic[tdcode] = ['']
                    else:
                        continue
                else:
                    continue
            mgframe = pd.DataFrame(mgdic)
            mgframe['userid'] = mgframe['userid'].astype('str')
            for col in mgframe.columns:
                mgframe[col] = mgframe[col].astype('str')
            qmframe = qmframe.merge(mgframe, left_on='userid', right_on='userid', left_index=True, how='left')
            # qmframe.iloc[:,1:].to_csv(self.tongduninfo_txt,sep='^',mode='a',index=False,header=False)

    def rds_contactlst(self):
        sfile = '/home/yanghan/risk_dataSet/data/source/RDS_teloperator_source_20171011.txt'
        outfile = '/home/yanghan/risk_dataSet/data/result/RDS_teloperator_20171011.txt'
        wfp = open(outfile, 'w')
        with open(sfile, 'r') as fp:
            linenum = 0
            for line in fp:
                linenum += 1
                # print linenum
                if linenum > 1:
                    linelist = line.strip().split('^')
                    linetemp = ''
                    for i in range(0, len(linelist) - 1):
                        linetemp += linelist[i] + '^'
                    if linelist[len(linelist) - 1] != '':
                        try:
                            cusjsonlst = json.loads(linelist[len(linelist) - 1])
                        except:
                            cusjsonlst = []
                        allpay = 0
                        if len(cusjsonlst) > 0:
                            for n in range(0, len(cusjsonlst)):
                                cusjson = cusjsonlst[n]
                                # print 'test'
                                if 'callPay' in cusjson:
                                    allpay += cusjsonlst[n]['callPay']
                                else:
                                    allpay += 0
                            avgpay = round(allpay / len(cusjsonlst), 4)
                        else:
                            avgpay = 0
                        wfp.write(linetemp + str(avgpay) + '\n')
                    else:
                        wfp.write(linetemp + '\n')
                else:
                    continue

    # 银联_8月前
    def rds_unioninfo_1(self):
        wudb = self.get_mq_conn()
        try:
            alluidframe = pd.read_sql(self.wecash_union_1, wudb)
        except:
            print "execute sqlfile1 error"
        alluid = tuple(alluidframe.userid)
        istart = 0
        igap = 100000
        uidtmp = ()
        while istart < len(alluid):
            uidtmp = alluid[istart:istart + igap]
            istart += igap
            if len(uidtmp) == 1:
                uidtmp += (-1,)
            self.wecash_union_2_tmp = self.wecash_union_2.format(user_id=uidtmp)
            wuframe = pd.read_sql(self.wecash_union_2_tmp, wudb)
            jsondic = collections.OrderedDict()
            jsondic.clear()
            for frameidx, framerow in wuframe.iterrows():
                if framerow.resultdetail != None:
                    jsonstr = json.loads(framerow.resultdetail)
                    if 'data' in jsonstr and 'unionpay_score' in jsonstr['data'] \
                            and 'origin' in jsonstr['data']['unionpay_score'][0] \
                            and 'result' in jsonstr['data']['unionpay_score'][0]['origin'] \
                            and 'quota' in jsonstr['data']['unionpay_score'][0]['origin']['result']:
                        for cd in self.wecashunion_lst:
                            dv = ''
                            if cd in jsonstr['data']['unionpay_score'][0]['origin']['result']['quota']:
                                dv = jsonstr['data']['unionpay_score'][0]['origin']['result']['quota'][cd]
                            if cd in jsondic:
                                jsondic[cd].append(str(dv))
                            else:
                                jsondic[cd] = [str(dv)]
                else:
                    for cd in self.wecashunion_lst:
                        if cd in jsondic:
                            jsondic[cd].append('')
                        else:
                            jsondic[cd] = ['']
            josnframe = pd.DataFrame(jsondic)
            lastframe = pd.concat([wuframe.iloc[:, :-1], josnframe], axis=1, join_axes=[wuframe.iloc[:, :-1].index])
            lastframe.to_csv('/home/yanghan/risk_dataSet/data/RDS_wecashunion_20170731.txt', sep='^', mode='a',
                             index=False, header=False)
        wudb.close()


if __name__ == '__main__':
    t = Rds_Data()
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print "start"
    # t.rds_audit()    #审批结果
    # t.rds_repay()     #还款信息
    # t.rds_loan()      #借款信息
    # t.rds_custom()    #客户基本信息
    # t.rds_score()     #评分类
    # t.rds_wecashbehavior()   #闪银借贷行为
    # t.rds_unioninfo_2() #银联数据_8月后
    t.rds_tongdun()  # 同盾

    # t.rds_contactlst()   #通讯录授权
    # t.rds_unioninfo_1()   #银联数据_8月前
    print "finish"
    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

