# -*- encoding: utf-8 -*-
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
import db_conf_501 as dconf
import logging
import copy
import pandas as pd
class TongDundata():
    def __init__(self):
        self.mhost1 = dconf.mg_host1
        self.mhost2 = dconf.mg_host2
        self.mreplicat_set = dconf.mg_replicat_set
        self.mgdb=dconf.mg_db
        self.mgcollection=dconf.mg_collection
        self.mgcollectionrelation = dconf.mg_coll_relation
        self.mgcollection_tongdun = dconf.mg_coll_tonfdun
        self.mgcollectionrelation_tongdun_detail = dconf.mg_coll_tonfdun_detail
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.mgconn = None
        self.init_mg_conn()

    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost2], maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self,mc):
        # mc = self.mgcollection
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]
    def getbankjsondata(self,ts,ul):
        # print ts,ul['data']['unionpay_score'][0]['origin']['result']['quota']
        if ts in ul:
            tr = ul[ts]
            # print tr
            if tr == None:
                return str(0)
            else:
                return str(tr)
        else:
            return str(0)
    def gettongdundata(self,uid1,uid2):
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun)
        creldetail = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        rerel = crel.find({'_id': uid1})
        # print uid1
        rerel=list(rerel)

        if len(rerel) <1:
            return ['0','0','0']
        rereldetail = creldetail.find({'_id':uid2})
        rereldetail = list(rereldetail)
        # print rereldetail

        if len(rereldetail) <1:
            return ['0', '0', '0']
        # print rerel[0]

        # if 'policySet' not in rerel[0]:
        #     return ['0', '0', '0']

        if 'policySet' not in rerel[0]:
            return ['0', '0', '0']
        pojson=json.loads(rerel[0]['policySet'])
        # print pojson
        day7=0
        day1=0
        day3=0
        num7=0
        num1=0
        num3=0
        if 'hit_rules' not in pojson[0]:
            return ['0', '0', '0']
        for po in pojson[0]['hit_rules']:
            # print po
            if po['name']=='7天内申请人在多个平台申请借款':
                day7=po['id']
                continue
            if po['name']=='1个月内申请人在多个平台申请借款':
                day1=po['id']
                continue
            if po['name']=='3个月内申请人在多个平台申请借款':
                day3=po['id']
                continue
        # print day7,day1,day3
        # if 'ruleDetail' not in rereldetail:
        #     return ['0', '0', '0']
        # print rereldetail[0]['ruleDetail']
        rulejson=json.loads(rereldetail[0]['ruleDetail'])
        # print rulejson
        for ru in rulejson:
            if day7 == ru:
                for d in rulejson[day7]:
                    dl =d.strip().split(':')
                    num7+=int(dl[1])
                continue
            if day1 == ru:
                for d in rulejson[day1]:
                    dl =d.strip().split(':')
                    num1+=int(dl[1])
                continue
            if day3 == ru:
                for d in rulejson[day3]:
                    dl =d.strip().split(':')
                    # print dl
                    num3+=int(dl[1])
                continue
        return [str(num7),str(num1),str(num3)]

    def tongdun(self):
        sfile='/Users/ufenqi/Downloads/GOT_20170906_8月前.txt'
        outfile='/Users/ufenqi/Downloads/GOT_20170906_8月前'
        wfp=open(outfile,'w')
        with open(sfile,'r') as fp:
             for line in fp:
                liall=[]
                lall=[]
                linelist=line.strip().split('^')
                liall+=linelist[:-2]
                if linelist[-2]=='':
                    liall.append('0')
                    lall.append('0')
                else:
                    cc = json.loads(linelist[-2])
                    num=0
                    for ci in cc:
                        num+=ci['callPay']
                        # print num
                    liall.append(str(num*1.0/6))
                    lall.append(str(num*1.0/6))
                if linelist[-1]=='':
                    zl=[]
                    for i in range(0,20):
                        zl.append('0')
                    liall+=zl
                else:
                    # print linelist[-1]
                    ul = json.loads(linelist[-1])
                    try:
                        if 'quota' in ul['data']['unionpay_score'][0]['origin']['result']:
                            ul=ul['data']['unionpay_score'][0]['origin']['result']['quota']
                            liall.append(self.getbankjsondata(u'S0680',ul))
                            liall.append(self.getbankjsondata(u'S0544', ul))
                            liall.append(self.getbankjsondata(u'S0545', ul))
                            liall.append(self.getbankjsondata(u'S0549', ul))
                            liall.append(self.getbankjsondata(u'S0550', ul))
                            liall.append(self.getbankjsondata(u'S0552', ul))
                            liall.append(self.getbankjsondata(u'S0553', ul))
                            liall.append(self.getbankjsondata(u'S0359', ul))
                            liall.append(self.getbankjsondata(u'S0428', ul))
                            liall.append(self.getbankjsondata(u'S0629', ul))
                            if u'S0631' in ul:
                                tr = ul['S0631']
                                if tr == None:
                                    liall.append(str(0))
                                else:
                                    trlist=tr.strip().split(';')
                                    num = 0
                                    for ti in trlist:
                                        til=ti.strip().split('_')
                                        if til[1]=='NA':
                                            continue
                                        else:
                                            num+=int(til[1])
                                    liall.append(str(num))
                            else:
                                liall.append(str(0))
                            liall.append(self.getbankjsondata(u'S0636', ul))
                            liall.append(self.getbankjsondata(u'S0637', ul))
                            liall.append(self.getbankjsondata(u'S0331', ul))
                            liall.append(self.getbankjsondata(u'S0332', ul))
                            liall.append(self.getbankjsondata(u'S0334', ul))
                            liall.append(self.getbankjsondata(u'S0335', ul))
                            liall.append(self.getbankjsondata(u'S0056', ul))
                            liall.append(self.getbankjsondata(u'S0059', ul))
                            liall.append(self.getbankjsondata(u'S0102', ul))
                        else:
                            zl = []
                            for i in range(0, 20):
                                zl.append('0')
                            liall += zl
                    except:
                        zl = []
                        for i in range(0, 20):
                            zl.append('0')
                        lall += zl
                        liall=lall
                        # continue
                if linelist[0]=='' or linelist[1]=='':
                    liall+=['0','0','0']
                # print linelist[0],linelist[1]
                liall+=self.gettongdundata(linelist[0],linelist[1])
                liall.append(self.get_tongduntype(linelist[0]))
                wfp.write('^'.join(liall)+'\n')

    def tongdun_8(self):
        sfile='/Users/ufenqi/Downloads/GOT20170906_8月后.txt'
        outfile='/Users/ufenqi/Downloads/GOT20170906_8月后'
        wfp=open(outfile,'w')
        with open(sfile,'r') as fp:
             for line in fp:
                liall=[]
                linelist=line.strip().split('^')
                liall+=linelist[:-2]
                if linelist[-2]=='':
                    liall.append('0')
                else:
                    cc = json.loads(linelist[-2])
                    num=0
                    for ci in cc:
                        num+=ci['callPay']
                        # print num
                    liall.append(str(num*1.0/6))
                if linelist[-1]=='':
                    zl=[]
                    for i in range(0,20):
                        zl.append('0')
                    liall+=zl
                else:
                    # print linelist[-1]
                    c = self.get_mg_conn(dconf.mg_collection_detail)
                    cinfo = c.find_one({'_id': linelist[1]})
                    if not cinfo:
                        zl = []
                        for i in range(0, 20):
                            zl.append('0')
                        liall += zl
                        if linelist[0] == '' or linelist[1] == '':
                            liall += ['0', '0', '0']
                        # print linelist[0],linelist[1]
                        liall += self.gettongdundata(linelist[0], linelist[1])
                        liall.append(self.get_tongduntype(linelist[0]))
                        wfp.write('^'.join(liall) + '\n')
                        continue
                    if 'cardsStr' not in cinfo:
                        zl = []
                        for i in range(0, 20):
                            zl.append('0')
                        liall += zl
                        if linelist[0] == '' or linelist[1] == '':
                            liall += ['0', '0', '0']
                        # print linelist[0],linelist[1]
                        liall += self.gettongdundata(linelist[0], linelist[1])
                        liall.append(self.get_tongduntype(linelist[0]))
                        wfp.write('^'.join(liall) + '\n')
                        continue
                    ul=None
                    try:

                        ul = json.loads(cinfo['cardsStr'])
                        if linelist[-1] in ul:
                            uv = ul[linelist[-1]]
                            resultdetail = uv['resultDetail']
                            ul = json.loads(resultdetail)
                            # print resultdetail
                    except:
                        zl = []
                        for i in range(0, 20):
                            zl.append('0')
                        liall += zl
                        if linelist[0] == '' or linelist[1] == '':
                            liall += ['0', '0', '0']
                        # print linelist[0],linelist[1]
                        liall += self.gettongdundata(linelist[0], linelist[1])
                        liall.append(self.get_tongduntype(linelist[0]))
                        wfp.write('^'.join(liall) + '\n')
                        continue
                    liall.append(self.getbankjsondata(u'S0680',ul))
                    liall.append(self.getbankjsondata(u'S0544', ul))
                    liall.append(self.getbankjsondata(u'S0545', ul))
                    liall.append(self.getbankjsondata(u'S0549', ul))
                    liall.append(self.getbankjsondata(u'S0550', ul))
                    liall.append(self.getbankjsondata(u'S0552', ul))
                    liall.append(self.getbankjsondata(u'S0553', ul))
                    liall.append(self.getbankjsondata(u'S0359', ul))
                    liall.append(self.getbankjsondata(u'S0428', ul))
                    liall.append(self.getbankjsondata(u'S0629', ul))
                    if u'S0631' in ul:
                        tr = ul['S0631']
                        if tr == None:
                            liall.append(str(0))
                        else:
                            trlist=tr.strip().split(';')
                            num = 0
                            for ti in trlist:
                                til=ti.strip().split('_')
                                if til[1]=='NA':
                                    continue
                                else:
                                    num+=int(til[1])
                            liall.append(str(num))
                    else:
                        liall.append(str(0))
                    liall.append(self.getbankjsondata(u'S0636', ul))
                    liall.append(self.getbankjsondata(u'S0637', ul))
                    liall.append(self.getbankjsondata(u'S0331', ul))
                    liall.append(self.getbankjsondata(u'S0332', ul))
                    liall.append(self.getbankjsondata(u'S0334', ul))
                    liall.append(self.getbankjsondata(u'S0335', ul))
                    liall.append(self.getbankjsondata(u'S0056', ul))
                    liall.append(self.getbankjsondata(u'S0059', ul))
                    liall.append(self.getbankjsondata(u'S0102', ul))
                if linelist[0]=='' or linelist[1]=='':
                    liall+=['0','0','0']
                # print linelist[0],linelist[1]
                liall+=self.gettongdundata(linelist[0],linelist[1])
                liall.append(self.get_tongduntype(linelist[0]))
                wfp.write('^'.join(liall)+'\n')

    def get_tongduntype(self,uid):
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun)
        rerel = crel.find({'_id': uid})
        # print uid1
        rerel = list(rerel)

        if len(rerel) < 1:
            return ''
        if 'eventId' in rerel[0]:
            # print rerel[0]['eventId']
            return rerel[0]['eventId']
            # a.append(rerel[0]['eventId'])
            # wfp.write(','.join(a) + '\n')
                # break
    def tongduntype(self):
        sfile='/Users/ufenqi/Downloads/payday.csv'
        outfile='/Users/ufenqi/Downloads/payday.txt'
        wfp=open(outfile,'w')
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun)
        with open(sfile,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                a=linelist
                print a
                rerel = crel.find({'_id': linelist[0]})
                # print uid1
                rerel = list(rerel)

                if len(rerel) < 1:
                    continue
                if 'eventId' in rerel[0]:
                    a.append(rerel[0]['eventId'])
                    wfp.write(','.join(a)+'\n')
    def tongdun1(self):
        sfile = '/Users/ufenqi/Downloads/8rnac'
        outfile = '/Users/ufenqi/Downloads/tongdunc'
        wfp = open(outfile, 'w')
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun)
        with open(sfile, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                a = linelist
                # print a
                rerel = crel.find({'_id': linelist[0]})
                if rerel is None:
                    continue

                # print uid1
                try:
                    rerel = list(rerel)

                    if len(rerel) < 1:
                        continue
                    if 'finalScore' in rerel[0]:
                        a.append(str(rerel[0]['finalScore']))
                        wfp.write(','.join(a) + '\n')
                except:
                    continue
    def tongdun2(self):
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun)
        rerel = crel.find({'_id': {'$in':['4416590|H5','4444170|H5','4507780|H5','4516922|H5','4517206|H5','4519007|H5','4520468|H5','4527126|H5',
                                          '4530242|H5','4530882|H5','4531180|H5','4535465|H5','4536546|H5','4538174|H5','4543997|H5','4547834|H5',
                                          '4548710|H5','4629566|H5','4638616|H5','4639894|H5','4647285|H5','4649243|H5','4650336|H5','4650867|H5','4674644|H5','4705173|H5','4719328|H5','4742269|H5','4773078|H5','4778089|H5','4861081|H5','4895453|H5','4918001|H5','4923206|H5','546224|H5 ','4931024|H5','5172942|H5',
                                          '4527306|H5','4530887|H5','4624463|H5','4629608|H5','4630853|H5','4663687|H5','4866263|H5']}})
        rerel = list(rerel)
        for rel in rerel:
            if 'finalScore' in rel:
                print rel['_id'],rel['finalScore']

    def tongdun_name(self):
        sfile = '/Users/ufenqi/Downloads/tongdun_hitRules1.txt'
        outfile = '/Users/ufenqi/Downloads/tongdun_hitRules1'
        wfp = open(outfile, 'w')
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun)
        creldetail = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        with open(sfile,'r') as fp:
            for line in fp:
                linelist=line.strip().split('^')

                rerel = crel.find({'_id': linelist[0]})
                # print uid1
                rerel = list(rerel)
                strt=line.strip()
                if len(rerel) < 1:
                    wfp.write(strt+'^^'+'\n')
                    continue
                if 'createDate' not in rerel[0]:
                    strt=strt+'^'
                else:
                    strt=strt+'^'+rerel[0]['createDate']
                if 'policySet' not in rerel[0]:
                    strt = strt + '^'
                else:
                    pojson = json.loads(rerel[0]['policySet'])
                    if 'hit_rules' not in pojson[0]:
                        strt = strt + '^'
                    else:
                        nl=[]
                        for po in pojson[0]['hit_rules']:
                            if 'name' in po:
                                nl.append(po['name'])
                        strt = strt + '^'+','.join(nl)
                wfp.write(strt+'\n')
    def mongodb_test(self):
        crel = self.get_mg_conn(dconf.mg_coll_relation)
        # creldetail = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        ls='1514[0-9][0-9][0-9]9797'
        # ls='10020352|APP'
        print ls
        rerel = crel.find({'_id': {'$regex' :ls}})
        # rerel = crel.find({'_id': ls})
        # print uid1
        rerel = list(rerel)
        print rerel
        # if len(rerel) < 1:
        # if 'createDate' not in rerel[0]:
        #     strt=strt+'^'
        # else:
        #     strt=strt+'^'+rerel[0]['createDate']
        # if 'policySet' not in rerel[0]:
        #     strt = strt + '^'
        # else:
        #     pojson = json.loads(rerel[0]['policySet'])
        #     if 'hit_rules' not in pojson[0]:
        #         strt = strt + '^'
        #     else:
        #         nl=[]
        #         for po in pojson[0]['hit_rules']:
        #             if 'name' in po:
        #                 nl.append(po['name'])
        #         strt = strt + '^'+','.join(nl)
        # wfp.write(strt+'\n')
    def get_user_tongdun(self):
        sfile = '/Users/ufenqi/Downloads/qunar.txt'
        outfile = '/Users/ufenqi/Downloads/qunar'
        wfp = open(outfile, 'w')
        crel = self.get_mg_conn(dconf.mg_coll_user_tonfdun)
        # creldetail = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        with open(sfile, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('^')
                # print linelist[0]
                rerel = crel.find({'_id': linelist[0]})
                # print uid1
                rerel = list(rerel)
                # print rerel
                strt = line.strip()
                if len(rerel) < 1:
                    # print '222'
                    wfp.write(strt + '^' + '\n')
                    continue
                if 'finalScore' not in rerel[0]:
                    strt = strt + '^'
                else:
                    # print rerel[0]['finalScore']
                    strt=strt+'^'+str(rerel[0]['finalScore'])
                wfp.write(strt + '\n')
                # break
    def get_user_qunar(self):
        sfile = '/Users/ufenqi/Downloads/userid.txt'
        outfile = '/Users/ufenqi/Downloads/userid'
        rowlist=['appUserId','product','callbackUrl','timestamp','idcard','name',
                 'sex','roomEveryNightAvgPrice','routineDegree','hotelPriceSense',
                 'consumeAbility','applyCreditScore','activeResponseScore','cancleOrderRate',
                 'refundRate','birthday','origin','marriedProbability','haveChildProbability',
                 'educatePredict','haveCarProbability','haveHouseProbability','qunarUserScore',
                 'scalperRate','internationalUserRate','familyAccountRate','isCreditCard',
                 'creditCardMaxAmount','airTicketConsumeAbility','airTicketPriceSensitive',
                 'airportTend','airTicketPricePreKm','hotelConsumeAbility','hotelTendOfCity',
                 'hotelTendOfBrand','trainTicketConsumeAbility','trainTicketPriceSensitive',
                 'trainTicketTendOfCity','trainTicketPricePreKm','trainTicketTendOfSeat',
                 'holidayConsumeAbility','holidayPriceSensitive','holidayTendOfCity','holiday',
                 'isNaquhuaBlacklist','fenqiTendOfRate','cuiResponseOfRate','breachOfContractRate',
                 'convertCashRate','cuiAbilityRate','loginFrequency','loginTimeTrend',
                 'orderPlateformTrend','holidayConsumeIntention','consumeStability','tripPlanDegree',
                 'payTrend','friendInfluence','obsessionDegree','actSense','qunarRelation',
                 'priceSense','appViscosity','SocialCredit','livingCity','livingStability',
                 'studentUserProbability','businessTravelUser']
        wfp = open(outfile, 'w')
        crel = self.get_mg_conn(dconf.mg_coll_user_qunar)
        # creldetail = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        uidlist=[]
        i=0
        with open(sfile, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('^')
                if i>1000:
                    rerel = crel.find({'_id': {'$in':uidlist}})
                    for re in rerel:

                        allstr = re['_id']
                        try:
                            jsons=re['userInfo']
                            jsons=json.loads(jsons)
                            for row in rowlist:
                                if row in jsons:
                                    allstr=allstr+'^'+str(jsons[row])
                                else:
                                    allstr = allstr + '^'
                            wfp.write(allstr+'\n')
                        except:
                            for i in range(0,len(rowlist)):
                                allstr = allstr+'^'
                            wfp.write(allstr+'\n')
                    i=0
                    uidlist=[]
                else:
                    uidlist.append(linelist[0])
                    i+=1
            if i<=1000:
                rerel = crel.find({'_id': {'$in': uidlist}})
                for re in rerel:
                    allstr = re['_id']
                    try:
                        jsons = re['userInfo']
                        jsons = json.loads(str(jsons))
                        # print jsons
                        for row in rowlist:
                            if row in jsons:
                                allstr = allstr + '^' + str(jsons[row])
                            else:
                                allstr = allstr + '^'
                        wfp.write(allstr + '\n')
                    except:
                        for i in range(0, len(rowlist)):
                            allstr = allstr + '^'
                        wfp.write(allstr + '\n')
    def finalScore(self):
        sfile = '/Users/ufenqi/Documents/dataming/base2/overdue/phn_uid.txt'
        s2file = '/Users/ufenqi/Documents/dataming/base2/overdue/score_all7_8'
        outfile = '/Users/ufenqi/Documents/dataming/base2/overdue/phn_score7_8'
        scoredic={}
        with open(s2file, 'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                linelist[1]=linelist[1][1:11]
                scoredic[linelist[0]]=linelist
        wfp = open(outfile, 'w')
        uidlist={}
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun)
        # creldetail = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        i=0
        n=0
        with open(sfile, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('^')
                if i > 1000:
                    rerel = crel.find({'_id': {'$in': uidlist.keys()}})
                    # print rerel.count()
                    for re in rerel:
                        allstr = re['_id']
                        if 'finalScore' in re:
                            fscore=re['finalScore']
                            phn1=uidlist[allstr]
                            if phn1 in scoredic:
                                uls=scoredic[phn1]
                                wfp.write(uls[0]+','+uls[1]+','+uls[2]+','+str(fscore)+','+uls[-1]+'\n')

                    i = 0
                    n+=1
                    print n
                    uidlist = {}
                else:
                    # print linelist[1]
                    uidlist[linelist[1]]=linelist[0]
                    i += 1
            if i <= 1000:
                rerel = crel.find({'_id': {'$in': uidlist.keys()}})
                for re in rerel:
                    allstr = re['_id']
                    if 'finalScore' in re:
                        fscore = re['finalScore']
                        phn1 = uidlist[allstr]
                        uls = scoredic[phn1]
                        wfp.write(uls[0] + ',' + uls[1] + ',' + uls[2] + ',' + str(fscore) +','+uls[-1] + '\n')
    def tongdunRuleName(self):
        sfile = '/Users/ufenqi/Documents/dataming/base2/overdue/phn_uid.txt'
        # s2file = '/Users/ufenqi/Documents/dataming/base2/overdue/phn_uid.txt'
        # s2file = '/Users/ufenqi/Documents/dataming/base2/overdue/score_all7_8'
        outfile = '/Users/ufenqi/Documents/dataming/base2/overdue/td_rule_feature_risk.csv'
        s_file ='/Users/ufenqi/Documents/dataming/base2/overdue/score_td_715'
        # phncontent = {}
        allphndic={}
        namelist=[u'1天内身份证使用过多设备进行申请',u'7天内身份证使用过多设备进行申请',
                  u'7天内申请人在多个平台申请借款',u'1个月内申请人在多个平台申请借款',
                  u'1天内设备使用过多的身份证或手机号进行申请',u'身份证命中低风险关注名单',
                  u'手机号命中低风险关注名单',u'身份证命中中风险关注名单',
                  u'手机号命中中风险关注名单',u'3个月内身份证关联多个申请信息'
                  ]
        # namelist=[u'身份证命中低风险关注名单',u'手机号命中低风险关注名单',
        #           u'身份证命中中风险关注名单',u'手机号命中中风险关注名单',
        #           u'3个月内身份证关联多个申请信息'
        #           ]
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                allphndic[linelist[0]] = linelist
        # namedic = {}
        # with open(s2file, 'r') as fp:
        #     for line in fp:
        #         linelist = line.strip().split(',')
        #         linelist[1] = linelist[1][1:11]
        #         scoredic[linelist[0]] = linelist
        # wfp = open(outfile, 'w')
        uidlist = {}
        featurelist=[]
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        # creldetail = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        i = 0
        n = 0
        with open(sfile, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('^')
                if i > 1000:
                    rerel = crel.find({'_id': {'$in': uidlist.keys()}})
                    # print rerel.count()
                    for re in rerel:
                        allstr = re['_id']
                        namedic = {}
                        if 'viewDetail' in re:
                            fscore = re['viewDetail']
                            viewjson = json.loads(fscore)
                            # print viewjson[0]
                            if 'rules' in viewjson[0]:
                                viewrule = viewjson[0]['rules']
                                for rule in viewrule:
                                    if 'rule_name' in rule:
                                        if rule['rule_name'] in namelist:
                                            if rule['rule_name'] in [u'1天内设备使用过多的身份证或手机号进行申请',
                                                                     u'1天内身份证使用过多设备进行申请',
                                                                     u'7天内身份证使用过多设备进行申请',
                                                                     u'3个月内身份证关联多个申请信息']:

                                                if 'rule_detail' in rule:
                                                    for rd in rule['rule_detail']:
                                                        if '关联的个数' in rd:
                                                            lnumlist = rd.strip().split(':')
                                                            namedic.setdefault(rule['rule_name'], 0)
                                                            # print allstr, rule['rule_name'], float(lnumlist[1])
                                                            namedic[rule['rule_name']] += float(lnumlist[1])

                                            elif rule['rule_name'] in [u'身份证命中低风险关注名单',u'手机号命中低风险关注名单',
                                                                     u'身份证命中中风险关注名单',u'手机号命中中风险关注名单'
                                                                     ]:
                                                namedic[rule['rule_name']] = 1
                                            else:
                                                if 'rule_detail' in rule:
                                                    for nai in rule['rule_detail']:
                                                        # print
                                                        lnumlist = nai.strip().split(':')
                                                        namedic.setdefault(rule['rule_name'], 0)
                                                        # print allstr
                                                        # print allstr, rule['rule_name'], lnumlist[0], float(lnumlist[1])
                                                        namedic[rule['rule_name']] += float(lnumlist[1])
                        if len(namedic) > 0:
                            phn = uidlist[allstr]
                            # print phn
                            flist = allphndic[phn]
                            # print flist
                            # for k in namedic.keys():
                                # print k
                            for ni in namelist:
                                # print '111', ni
                                if ni in namedic:
                                    flist.append(namedic[ni])
                                else:
                                    flist.append(-1)
                            featurelist.append(flist)

                    i = 0
                    n += 1
                    print n
                    uidlist = {}
                else:
                    # print linelist[1]
                    list2=linelist[1].strip().split('|')
                    if linelist[0] in allphndic:
                        # print 'phn',linelist[0]
                        uidlist[list2[0]] = linelist[0]
                        i += 1
            if i <= 1000:
                rerel = crel.find({'_id': {'$in': uidlist.keys()}})
                # print rerel.count()
                for re in rerel:
                    allstr = re['_id']
                    namedic = {}
                    if 'viewDetail' in re:
                        fscore = re['viewDetail']
                        viewjson = json.loads(fscore)
                        # print viewjson[0]
                        if 'rules' in viewjson[0]:
                            viewrule = viewjson[0]['rules']
                            for rule in viewrule:
                                if 'rule_name' in rule:
                                    if rule['rule_name'] in namelist:
                                        if rule['rule_name'] in [u'1天内设备使用过多的身份证或手机号进行申请',
                                                                 u'1天内身份证使用过多设备进行申请',
                                                                 u'7天内身份证使用过多设备进行申请',
                                                                 u'3个月内身份证关联多个申请信息']:

                                            if 'rule_detail' in rule:
                                                for rd in rule['rule_detail']:
                                                    if '关联的个数' in rd:
                                                        lnumlist = rd.strip().split(':')
                                                        namedic.setdefault(rule['rule_name'], 0)
                                                        # print allstr, rule['rule_name'], float(lnumlist[1])
                                                        namedic[rule['rule_name']] += float(lnumlist[1])

                                        elif rule['rule_name'] in [u'身份证命中低风险关注名单', u'手机号命中低风险关注名单',
                                                                   u'身份证命中中风险关注名单', u'手机号命中中风险关注名单'
                                                                   ]:
                                            namedic[rule['rule_name']] = 1
                                        else:
                                            if 'rule_detail' in rule:
                                                for nai in rule['rule_detail']:
                                                    # print
                                                    lnumlist = nai.strip().split(':')
                                                    namedic.setdefault(rule['rule_name'], 0)
                                                    # print allstr
                                                    # print allstr, rule['rule_name'], lnumlist[0], float(lnumlist[1])
                                                    namedic[rule['rule_name']] += float(lnumlist[1])
                    if len(namedic) > 0:
                        phn = uidlist[allstr]
                        # print phn
                        flist = allphndic[phn]
                        # print flist
                        # for k in namedic.keys():
                            # print k
                        for ni in namelist:
                            # print '111',ni
                            if ni in namedic:
                                flist.append(namedic[ni])
                            else:
                                flist.append(-1)
                        featurelist.append(flist)
                        # namedic.setdefault(rule['rule_name'],0)
                        # namedic[rule['rule_name']]+=1
        # namelist=[]
        # for name in namedic:
        #     # print name,namedic[name]
        #     nl=[namedic[name],name]
        #     namelist.append(nl)
        featuredf=pd.DataFrame(featurelist,columns=['phn', 'daytime', 'myscore', 'tdscore', 'overday',
                                                    'identity_1', 'identity_7','apply_7','apply_30','facility_1',
                                                    'identity_d', 'phn_d','identity_z','phn_z','relevance'])
        # featuredf = pd.DataFrame(featurelist, columns=['phn', 'daytime', 'myscore', 'tdscore', 'overday',
        #                                                'identity_d', 'phn_d','identity_z','phn_z'])
        featuredf.to_csv(outfile,index=False)
    def tongdun_name_num(self):
        sfile = '/Users/ufenqi/Documents/dataming/base2/overdue/phn_uid.txt'
        s_file = '/Users/ufenqi/Documents/dataming/base2/overdue/score_td_715'
        outfile='/Users/ufenqi/Documents/dataming/base2/overdue/td_name_num.csv'
        allphndic = {}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                allphndic[linelist[0]] = linelist
        crel = self.get_mg_conn(dconf.mg_coll_tonfdun_detail)
        i = 0
        n = 0
        namedic={}
        uidlist = {}
        with open(sfile, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('^')
                if i > 1000:
                    rerel = crel.find({'_id': {'$in': uidlist.keys()}})
                    # print rerel.count()
                    for re in rerel:
                        allstr = re['_id']
                        if 'viewDetail' in re:
                            fscore = re['viewDetail']
                            viewjson = json.loads(fscore)
                            # print viewjson[0]
                            if 'rules' in viewjson[0]:
                                viewrule = viewjson[0]['rules']
                                for rule in viewrule:
                                    if 'rule_name' in rule:
                                        # print rule['rule_name']
                                        namedic.setdefault(rule['rule_name'],0)
                                        namedic[rule['rule_name']]+=1
                    i = 0
                    n += 1
                    print n
                    uidlist = {}
                else:
                    # print linelist[1]
                    list2 = linelist[1].strip().split('|')
                    if linelist[0] in allphndic:
                        # print 'phn',linelist[0]
                        uidlist[list2[0]] = linelist[0]
                        i += 1
        if i <= 1000:
            rerel = crel.find({'_id': {'$in': uidlist.keys()}})
            # print rerel.count()
            for re in rerel:
                allstr = re['_id']
                if 'viewDetail' in re:
                    fscore = re['viewDetail']
                    viewjson = json.loads(fscore)
                    # print viewjson[0]
                    if 'rules' in viewjson[0]:
                        viewrule = viewjson[0]['rules']
                        for rule in viewrule:
                            if 'rule_name' in rule:
                                namedic.setdefault(rule['rule_name'], 0)
                                namedic[rule['rule_name']] += 1
        namelist=[]
        for name in namedic:
            namelist.append([name,namedic[name]])
        namedf=pd.DataFrame(namelist,columns=['name','num'])
        namedf.to_csv(outfile, index=False)
if __name__ == '__main__':
    print 111
    t=TongDundata()
    # 8月前
    # t.tongdun()
    # 8月后
    # t.tongdun_8()
    # t.tongduntype()
    # t.tongdun1()
    # t.tongdun2()
    # t.tongdun_name()
    # t.mongodb_test()
    # t.get_user_tongdun()
    # t.get_user_qunar()
    # t.finalScore()
    # t.tongdunRuleName()
    t.tongdun_name_num()