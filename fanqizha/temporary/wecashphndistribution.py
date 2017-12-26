# -*- encoding: utf-8 -*-
import sys
import time
import db_conf_501 as dconf
import pandas as pd
import pymongo as pm

reload(sys)
sys.setdefaultencoding('utf8')
class Wecashphnn(object):
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


    def get_wecash_phn(self):
        wecashyellowdf = pd.read_excel('/Users/ufenqi/Downloads/wecashyellowpage.xlsx')
        wecashyellowdf.columns = ['phone', 'name']
        wecashyellowdf['phone']=wecashyellowdf['phone'].astype('string')
        wecashmobile = wecashyellowdf['phone'].values.tolist()
        wecashname = wecashyellowdf['name'].values.tolist()
        phndic1 = {}
        for i in range(0, len(wecashmobile)):
            phn = wecashmobile[i]
            name = wecashname[i]
            phndic1.setdefault(phn, [])
            phndic1[phn].append(name)
        # for p in phndic:
        #     print p, phndic[p][0]
        # print len(wecashmobile)
        # print len(phndic)
        self.phndic=phndic1
        print len(self.phndic)
        print self.phndic.keys()
    def get_wecash_yellow_number(self,callre):
        callinfo = callre['call_info']
        phndic1=self.phndic
        num=0
        namedic={}
        for ci in callinfo:
            try:
                p = ci['phone_num']
                # print p
                if p in phndic1:
                    for name in phndic1[p]:
                        num+=1
                        namedic.setdefault(name,0)
                        namedic[name]+=1
            except Exception, e:
                pass
        return num,namedic
    def get_yellowpage_fea(self,phn):
        c = self.get_mg_conn(dconf.mg_collection)
        cinfo = c.find_one({'_id': phn})
        if cinfo is None:
            return 0,{}
        try:
            if 'call_details' in cinfo :
                num,namedic=self.get_wecash_yellow_number(cinfo['call_details'])
            return num,namedic
        except Exception, e:
            return 0, {}
    def get_all_feature(self):
        cf = self.get_mg_conn_yellow(dconf.mg_coll_fea)
        self.get_wecash_phn()
        # self.phndic['15752938813']=['aaa']
        init_file = '/Users/ufenqi/Documents/dataming/base1/data/modelscore501'
        userdic = {}
        with open(init_file, 'r')as fp:
            for line in fp:
                linelist = line.strip().split(',')
                userdic[linelist[0]] = linelist[3]
        print len(userdic)
        for phn in userdic:
            num,namedic= self.get_yellowpage_fea(phn)
            if num>0:
                print phn,userdic[phn],num
                for name in namedic:
                    print name,namedic[name]
                print '******************'



if __name__ == '__main__':
    w=Wecashphnn()
    w.get_all_feature()