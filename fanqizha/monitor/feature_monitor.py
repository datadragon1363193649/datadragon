#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
线上每天用户监控，包括：纪录每天用户每个特征平均值；纪录每天用户命中规则数c卡和闪银分比较
"""

import os
import sys
# import pandas as pda
import sys
import traceback
import json
import pymongo as pm
import time
import datetime
import offline_db_conf as dconf
import MySQLdb
import pandas as pd
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime
import logging
import copy
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=dconf.log_filename,
                filemode='a')
class FeatureMonitor(object):
    def __init__(self):
        self.mhost1 = dconf.mg_hosty1
        self.mhost2 = dconf.mg_hosty2
        # self.mreplicat_set = dconf.mg_replicat_set
        self.mgdb=dconf.mg_dby
        self.mgcollection=dconf.mg_collection
        self.mgcollectionrelation = dconf.mg_coll_relation
        self.mgfeature=dconf.mg_coll_feature
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
        self.mgconn = pm.MongoClient([ self.mhost2], maxPoolSize=10)
        self.mgconn[self.mgdb].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self,mc):
        # mc = self.mgcollection
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]
    def get_wecashAndSelfscore(self):
        mysqlcursor = self.mysqlconn.cursor()
        sql = 'select distinct rum.userid as "userid",uu.mobile as "phn", ' \
            'rcc.createDate as "time",rwc.contactScore AS "wecashscore",' \
            'fs.score AS "fraudScore",CASE WHEN rcc.`status` LIKE "refuse%" ' \
            'THEN "-3" WHEN rcc.`status` LIKE "pass%" AND bb.id IS NULL THEN "-2" ' \
            'WHEN rcc.`status` LIKE "pass%" and bb.id is not null AND ' \
            'bb.realPayDate IS NULL AND date(bb.repayEndDate) >= date(now()) THEN ' \
            '"-1" WHEN rcc.`status` LIKE "pass%" and bb.id is not null ' \
            'and bb.realPayDate is not null AND TO_DAYS(date(bb.realPayDate)) ' \
            '- TO_DAYS(date(bb.repayEndDate)) <= 0 THEN 0 ELSE datediff(ifnull(bb.real_paydate,curdate()),bb.repay_enddate) ' \
            'END AS "overdays",ifnull(wcru.rulenum,0) as "rulenum",' \
            'rcc.patternCode AS "patternCode" from  warehouse.risk_credit_chain rcc ' \
            'join warehouse.risk_user_merchant rum on rcc.userId=rum.userId and rum.merchantCode="1001" ' \
            'join warehouse.uc_users uu on rum.merchantUserId+0=uu.id and uu.userType=2 ' \
            'LEFT JOIN warehouse.risk_wecash_contact rwc ON rcc.userId = rwc.userId ' \
            'LEFT JOIN warehouse.risk_fraud_score fs ON rcc.userId = fs.userId ' \
            'LEFT JOIN warehouse.bc_bill bb ON rum.merchantUserId = bb.userId LEFT JOIN ( SELECT ' \
            't.userid AS userid,count(*) AS rulenum FROM warehouse.risk_auth_warnrule t WHERE ' \
            't.ruleCode <> "wecash_contact_score" and t.type=0 GROUP BY t.userId) ' \
            'wcru on rcc.userid=wcru.userid where rcc.pattern like "payday_basic%" and ' \
              'rcc.createDate>= SUBDATE(CURDATE(), 1) and rcc.createDate<CURDATE()'
        # sql = 'select distinct rum.userid as "userid",uu.mobile as "phn",' \
        #            'rcc.createDate as "time",rwc.contactScore AS "wecashscore",' \
        #            'fs.score AS "fraudScore",CASE WHEN rcc.status LIKE "refuse%" ' \
        #            'THEN "-3" WHEN rcc.status LIKE "pass%" AND bb.id IS NULL THEN "-2" ' \
        #            'WHEN rcc.status LIKE "pass%" and bb.id is not null AND ' \
        #            'bb.realPayDate IS NULL AND date(bb.repayEndDate) >= date(now()) THEN ' \
        #            '"-1" WHEN rcc.status LIKE "pass%" and bb.id is not null ' \
        #            'and bb.realPayDate is not null AND TO_DAYS(date(bb.realPayDate)) ' \
        #            '- TO_DAYS(date(bb.repayEndDate)) <= 0 THEN 0 ELSE bb.overdueDays ' \
        #            'END AS "overdays",ifnull(wcru.rulenum,0) as "rulenum" ,' \
        #             'rcc.patternCode AS "patternCode" from ' \
        #            'risk.risk_credit_chain rcc join risk.risk_user_merchant rum on' \
        #            ' rcc.userId=rum.userId and rum.merchantCode="1001" join ' \
        #            'sudaibear.uc_users uu on rum.merchantUserId+0=uu.id and ' \
        #            'uu.userType=2 LEFT JOIN risk.risk_wecash_contact rwc ON ' \
        #            'rcc.userId = rwc.userId LEFT JOIN risk.risk_fraud_score fs ON ' \
        #            'rcc.userId = fs.userId LEFT JOIN sudaibear.bc_bill_all bb ON ' \
        #            'rum.merchantUserId = bb.userId LEFT JOIN ( SELECT t.userid AS userid, ' \
        #            'count(*) AS rulenum FROM risk.risk_auth_warnrule t WHERE t.ruleCode ' \
        #            '<> "wecash_contact_score" and t.type=0 GROUP BY t.userId ) wcru on ' \
        #            'rcc.userid=wcru.userid where rcc.pattern like "payday_basic%" and ' \
        #            'rcc.createDate>= SUBDATE(CURDATE(), 1) and rcc.createDate<CURDATE()'
        # SUBDATE(CURDATE(), 1),CURDATE()
        try:
            self.mysqlconn.ping()
        except:
            self.init_mysql_conn()
            mysqlcursor = self.mysqlconn.cursor()
        n = mysqlcursor.execute(sql)
        userinfdic={}
        if n != 0:
            content = mysqlcursor.fetchall()
            for fli in content:
                if fli[1] in userinfdic:
                    if fli[5]=='':
                        continue
                    try:
                        if float(fli[5])>float(userinfdic[fli[1]][5]):
                            userinfdic[fli[1]]=fli
                    except:
                        continue
                else:
                    userinfdic[fli[1]] = fli
                # userid = fli[0]
                # phn=fli[1]
                # time=fli[2]
                # wecashscore=fli[3]
                # fraudScore=fli[4]
                # overdays=fli[5]
                # rulenum=fli[6]
        phnlist=userinfdic.keys()
        userlist=userinfdic.values()
        logging.info('The number of phnlist is :%s',  len(phnlist))
        return phnlist,userlist
    def ruleDistribution(self,userlist):
        nowdate=datetime.datetime.now()
        yestoday=nowdate - datetime.timedelta(days=1)
        out_file='/home/xuyonglong/ruledis/modelmonitor/ruledistribution/'+yestoday.strftime('%Y-%m-%d')
        wfp=open(out_file,'w')
        def rulenum(aucpassw,aucpassm):
            rulenumwdf = aucpassw['rulenum'].value_counts()
            indexlistw = rulenumwdf.index.tolist()
            rulenumlistw = rulenumwdf.values.tolist()
            rulenummdf = aucpassm['rulenum'].value_counts()
            indexlistm = rulenummdf.index.tolist()
            rulenumlistm = rulenummdf.values.tolist()
            lenw = len(indexlistw)
            lenm = len(indexlistm)
            if lenw>lenm:
                for i in range(0,lenw):
                    if i >=lenm:
                        wfp.write(str(indexlistw[i]) + ',' + str(rulenumlistw[i]) +
                                  '******* null , null \n')
                    else:
                        wfp.write(str(indexlistw[i]) + ',' + str(rulenumlistw[i])
                                  + '*******'+str(indexlistm[i]) + ',' +
                                  str(rulenumlistm[i])+'\n')
            else:
                for i in range(0,lenm):
                    if i >=lenw:
                        wfp.write('null , null ******* ' +str(indexlistm[i]) + ',' +
                                  str(rulenumlistm[i])+'\n')
                    else:
                        wfp.write(str(indexlistw[i]) + ',' + str(rulenumlistw[i])
                                  + '*******'+str(indexlistm[i]) + ',' +
                                  str(rulenumlistm[i])+'\n')
            # for i in range(0,len(indexlist)):
            #     wfp.write(str(indexlist[i])+','+str(rulenumlist[i])+'\n')
            #     print indexlist[i],rulenumlist[i]
        def ruledis(aucdfl):
            a = [2, 4, 6, 8]
            aucdfw = aucdfl.sort_values(by='wecashscore', ascending=False)
            aucdfm = aucdfl.sort_values(by='fraudScore', ascending=False)
            # print aucdfw.head()
            # print aucdfm.head()
            for i in a:
                aucpassw1 = aucdfw.head(int((len(aucdfw) * i) / 10))
                aucpassw0 = aucdfw.drop(aucpassw1.index)
                wfp.write('passrate: '+str(i)+',passnum: '+str(len(aucpassw1))+'\n')
                # print 'passrate', i,'passnum',len(aucpassw1)
                wfp.write('+++++++++++++++++++++\n')
                # print '+++++++++++++++++'
                aucpassm1 = aucdfm.head(int((len(aucdfm) * i) / 10))
                aucpassm0 = aucdfm.drop(aucpassm1.index)
                wfp.write('wecash pass user ******* fraudScore pass user \n')
                # print 'wecash pass user'
                rulenum(aucpassw1,aucpassm1)
                wfp.write('+++++++++++++++++++++\n')
                wfp.write('wecash refuse user ******* fraudScore refuse user \n')
                # print 'wecash pass user'
                rulenum(aucpassw0,aucpassm0)
                wfp.write('+++++++++++++++++++++\n')
                # wfp.write('fraudScore pass user \n')
                # # print 'wecash pass user'
                # rulenum(aucpassm1)
                # wfp.write('+++++++++++++++++++++\n')
                # wfp.write('fraudScore refuse user \n')
                # # print 'wecash pass user'
                # rulenum(aucpassm0)
                # rulenumdf0=aucpassw0['rulenum'].value_counts()
                # indexlist = rulenumdf0.index.tolist()
                # rulenumlist=rulenumdf0.values.tolist()

                # print 'wecash '
                # print aucpassw1['rulenum'].value_counts()
                # print 'myself bad user'
                # print  aucpassm0['rulenum'].value_counts()
                # print 'myself '
                # print  aucpassm1['rulenum'].value_counts()
                # print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
            # print '************************************************'
        aucdf = pd.DataFrame(userlist,
                             columns=['userid', 'phn', 'time', 'wecashscore', 'fraudScore',
                                      'overdays', 'rulenum','patternCode'])
        # print aucdf.head()
        aucdf['overdays'] = aucdf['overdays'].astype('float')
        aucdf['fraudScore'] = aucdf['fraudScore'].astype('float')
        aucdf['wecashscore'] = aucdf['wecashscore'].astype('float')
        aucdf['overdays1'] = aucdf['overdays']
        aucdfa=aucdf[aucdf['patternCode']=='B']
        aucdfc = aucdf[aucdf['patternCode'] == 'C']
        wfp.write('B card rule distribution length：'+str(len(aucdfa))+'\n')
        ruledis(aucdfa)
        wfp.write('************************************************\n')
        wfp.write('C card rule distribution length：'+str(len(aucdfc))+'\n')
        ruledis(aucdfc)
    def baseFeatureDistribution(self,phnlist):
        s_file = '/home/xuyonglong/ruledis/featurename_online'
        traindf=pd.read_csv('/home/xuyonglong/ruledis/modelmonitor/basefeature/train.csv')
        # print traindf.columns
        # print traindf['mean']
        traindf.index=traindf['featurename']
        nowdate = datetime.datetime.now()
        yestoday = nowdate - datetime.timedelta(days=1)
        yestoday=yestoday.strftime('%Y-%m-%d')
        feaidlist = []
        feanamelist = []
        # wfp = open(out_file, 'w')
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist.append(linelist[1])
        cf = self.get_mg_conn(dconf.mg_coll_feature)

        rs = cf.find({'_id': {'$in': phnlist}})
        # rs = cf.find({'_id':'18601222524'})
        rsl = list(rs)
        calldata = []
        for rl in rsl:
            clist = []
            for fn in feanamelist:
                if fn in rl['call']:
                    # print fn,str(rl['call'][fn])
                    clist.append(str(rl['call'][fn]))
                else:
                    clist.append(str(-1))
            calldata.append(clist)
        fdf = pd.DataFrame(calldata)
        logging.info('The number of fdf is :%s', len(fdf))
        logging.info('The number of fdf.columns is :%s', len(fdf.columns))
        fdf.columns = feanamelist
        for col in feanamelist:
            fdf[col] = fdf[col].astype('float64')
        desdf=fdf.describe().T
        desdf['featurename']=desdf.index
        # print desdf.columns
        col=desdf.columns.tolist()
        desdf=desdf[col[-1:]+col[:-1]]
        desdf['meangap']=desdf['mean']-traindf['mean']
        # print desdf['mean']
        # print desdf['meangap']
        desdf.to_csv('/home/xuyonglong/ruledis/modelmonitor/basefeature/'+yestoday+'.csv',index=False)
if __name__ == '__main__':
    fm=FeatureMonitor()
    phnlist, userlist=fm.get_wecashAndSelfscore()
    # print len(phnlist),len(userlist)
    fm.ruleDistribution(userlist)
    fm.baseFeatureDistribution(phnlist)
