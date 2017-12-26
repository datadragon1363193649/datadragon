# -*- encoding: utf-8 -*-
import sys
import time
import db_conf_501 as dconf
import pandas as pd
import pymongo as pm
import json
import numpy as np

reload(sys)
sys.setdefaultencoding('utf8')
class Bqsrule(object):
    def __init__(self):
        self.mhost2 = dconf.mg_host2
        self.mhosty1 = dconf.mg_hosty1
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.conny = None
        self.init_mg_conn_yellowpage()
        self.phndic={}
        self.conn = None
        self.init_mg_conn()
    def get_mg_conn_yellow(self, mc):
        mdb = self.conny[dconf.mg_dby]
        return mdb[mc]
    def init_mg_conn_yellowpage(self):
        self.conny = pm.MongoClient([self.mhosty1], maxPoolSize=10)
    def init_mg_conn(self):
        self.conn = pm.MongoClient([self.mhost2], maxPoolSize=10)
        self.conn[dconf.mg_db].authenticate(self.muname, self.mpasswd)
    def get_mg_conn(self, mc):
        mdb = self.conn[dconf.mg_db]
        return mdb[mc]
    def get_featurename(self):
        s_file='/Users/ufenqi/Documents/bank_phn.txt'
        out_file='/Users/ufenqi/Documents/bank_new_featurename'
        wfp=open(out_file,'w')
        n1=0
        phnlist=[]
        namedic = {}
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                phnlist.append(linelist[0])
                n1+=1
                if n1%100==0:
                    c = self.get_mg_conn(dconf.mg_collection_detail)
                    re=c.find({'mobile':{'$in':phnlist}})
                    rslrel = list(re)

                    for ri in rslrel:
                        if 'cardsStr' not in ri:
                            continue
                        c=ri['cardsStr']
                        # print c
                        try:
                            c = json.loads(c)
                            for uj in c:
                                # print uj
                                uv = c[uj]
                                if 'bankType' in uv and 'resultDetail' in uv:
                                    if uv['bankType'] == '1':
                                        resultdetail = uv['resultDetail']
                                        rejson = json.loads(resultdetail)
                                        for rek in rejson:
                                            namedic.setdefault(rek,0)
                                            namedic[rek]+=1
                        except Exception, e:
                            # print ri['mobile'],e
                            continue

                        # for ci in c:
                        #     if 'hitRules' not in ci:
                        #         continue
                        #     hitRules=ci['hitRules']
                        #     for hit in hitRules:
                        #         if 'ruleId' in hit and 'ruleName' in hit:
                        #             ruleid=hit['ruleId']
                        #             rulename=hit['ruleName']
                        #             ruledic[ruleid]=rulename
                    phnlist=[]
                    # print n1
                    print len(namedic)
                if n1>20000:
                    break
        for ru in namedic:
            wfp.write(str(ru)+','+str(namedic[ru])+'\n')
    def get_bank_feature(self):
        s_file = '/Users/ufenqi/Documents/dataming/base2/bank_new_featurename'
        s2_file = '/Users/ufenqi/Documents/dataming/base2/query_result_bank_new_feature.csv'
        out_file = '/Users/ufenqi/Documents/dataming/base2/bank_new_feature'
        outdf_file = '/Users/ufenqi/Documents/dataming/base2/bank_new_feature.csv'
        out2_file='/Users/ufenqi/Documents/dataming/base2/featurename_num'
        wfp = open(out_file, 'w')
        wfp2=open(out2_file,'w')
        namedic={}
        with open(s_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                namedic[linelist[0]]=0
        print len(namedic)
        columnslist=['0','1']
        columnslist+=namedic.keys()
        featuredf=[]
        with open(s2_file,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                # phnscore_dic[linelist[0]]=linelist[1]
                c = self.get_mg_conn(dconf.mg_collection_detail)
                re = c.find({'mobile': linelist[0]})
                ri=list(re)
                # print ri
                if 'cardsStr' not in ri[0]:
                    # print '111'
                    continue
                c = ri[0]['cardsStr']
                # print c
                try:
                    c = json.loads(c)
                    for uj in c:
                        # print uj
                        uv = c[uj]

                        if 'bankType' in uv and 'resultDetail' in uv:
                            if uv['bankType'] == '1':
                                featurelist = []
                                resultdetail = uv['resultDetail']
                                rejson = json.loads(resultdetail)
                                for name in namedic:
                                    if name in rejson:
                                        featurelist.append(str(rejson[name]))
                                        namedic[name]+=1
                                    else:
                                        featurelist.append('-1')
                                # featuredf.append(linelist+featurelist)
                                wfp.write(linelist[0]+'\t'+linelist[1]+'\t'+'\t'.join(featurelist)+'\n')

                except Exception, e:
                    # print ri['mobile'],e
                    continue
                # break
        # tdf=pd.DataFrame(featuredf,columns=columnslist)
        # tdf.to_csv(outdf_file,index=False)
        for name in namedic:
            wfp2.write(name+','+str(namedic[name])+'\n')
    def get_rulenum(self):
        s_file = '/Users/ufenqi/Documents/phnwe'
        s2_file='/Users/ufenqi/Documents/baqrule/baqrule'
        ruleidlist=[]
        rulenamelist=[]
        rulenamelist.append('phn')
        rulenamelist.append('target')
        with open(s2_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                ruleidlist.append(linelist[0])
                rulenamelist.append(linelist[1])
        phnlist = []
        phndic={}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                phndic[linelist[0] + '|verify']=int(linelist[3])
                phnlist.append(linelist[0] + '|verify')
        c = self.get_mg_conn(dconf.mg_collectionblack)
        re = c.find({'_id': {'$in': phnlist}})
        rslrel = list(re)
        rulenumlist=[]
        for ri in rslrel:
            rulenumdic={}
            if 'strategySet' not in ri:
                continue

            c = ri['strategySet']
            # print c
            try:
                c = json.loads(c)
                # print c
            except Exception, e:
                sys.stderr.write('exp: ' +ri['_id']+" "+ str(e) + '\n')
                continue

            for ci in c:
                if 'hitRules' not in ci:
                    continue
                hitRules=ci['hitRules']
                for hit in hitRules:
                    if 'ruleId' in hit and 'ruleName' in hit:
                        ruleid=hit['ruleId']
                        score=hit['score']
                        rulenumdic[ruleid]=score
            rlist = []
            rlist.append(ri['_id'])
            rlist.append(phndic[ri['_id']])
            for  rid in ruleidlist:
                if rid in rulenumdic:
                    rlist.append(rulenumdic[rid])
                else:
                    rlist.append(-1)
            rulenumlist.append(rlist)
        rulenumdf=pd.DataFrame(rulenumlist,columns=rulenamelist)
        # print rulenumdf.head()
        rulenumdf.to_csv('/Users/ufenqi/Documents/baqrule/rulenumdf.csv',index=False)

    def sort_by_value(self,d):
        items = d.items()
        backitems = [[v[1], v[0]] for v in items]
        backitems.sort()
        return [backitems[i][1] for i in range(0, len(backitems))]
    def bqsdistribution(self):
        ruledf = pd.read_csv('/Users/ufenqi/Documents/baqrule/rulenumdf.csv')
        # print ruledf.dtypes
        columnlist = ruledf.columns
        ulen=len(ruledf)
        num=0
        collist=[]
        collist.append('target')
        scoredic={}
        for col in columnlist[2:]:
            if len(ruledf['target'][ruledf[col] > -1])<40:
                # print col
                continue
            # ruledf.mean
            # print col
            # print ruledf['target'][ruledf[col] > -1]
            # print ruledf['target'].mean()
            print col
            print '命中规则逾期天数平均值 ' ,ruledf['target'][ruledf[col]>-1].mean()
            print '没有命中规则逾期天数平均值 ' ,ruledf['target'][ruledf[col] == -1].mean()
            print '数量 ' ,ruledf['target'][ruledf[col] > -1].__len__()
            print ruledf['target'][ruledf[col] > -1].describe()
            # print '｜｜｜｜｜｜｜｜｜｜｜｜｜｜｜｜｜'
            # print ruledf['target'].describe()
            # print a.sort_index()
            # print ru1

            scoredic[col]=abs(ruledf[col].corr(ruledf['target'],method='spearman'))

            # rucnt=ruledf[col].value_counts()
            # cntlist=rucnt.values.tolist()
            # sco=1.0*cntlist[0]/ulen
            # if sco<0.8:
            #     collist.append(col)
            #     num+=1

            # print rucnt
            # print 1.0 * cntlist[0] / ulen
        print 'num ',num
        namelist=self.sort_by_value(scoredic)
        # for na in namelist:
            # print na
        testdf=ruledf[collist]
        # print np.corrcoef(testdf, rowvar=0)

if __name__ == '__main__':
    b=Bqsrule()
    # b.get_featurename()
    b.get_bank_feature()
    # b.get_rulenum()
    # b.bqsdistribution()


