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


def get_mg_connlbs(mc):
    connlbs = pm.MongoClient(['172.16.51.13:3717'], maxPoolSize=10)
    connlbs[dconf.mg_db].authenticate('xuyonglong', 'MDkoWEYN3YhBNJpNLyVksdZA')
    mdb = 'risk'
    # mc = dconf.mg_collectionlbs
    mdb = connlbs['risk']
    return mdb[mc]
def bankj(phn):
    outfile='/Users/ufenqi/Documents/dataming/bank/'+phn+'.json'
    wfp=open(outfile,'w')
    c = get_mg_connlbs(dconf.mg_collection_detail)
    cinfo = c.find_one({'mobile': phn})
    ul = json.loads(cinfo['cardsStr'])
    for uj in ul:
        uv = ul[uj]
        if 'bankType' in uv and 'resultDetail' in uv:
            resultdetail = uv['resultDetail']
            rejson = json.loads(resultdetail)
            json.dump(rejson,wfp)
        break
def bankfea():
    outfile='/Users/ufenqi/Documents/dataming/bank/bank_fea_num_1031aa.csv'
    sfile='/Users/ufenqi/Documents/dataming/bank/score_1031aa'
    # wfp=open(outfile,'w')
    feadic = {}
    i=0
    rnum=0
    c = get_mg_connlbs(dconf.mg_collection_detail)
    with open(sfile,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')

            # print linelist
            cinfo = c.find_one({'mobile': linelist[0]})
            try:
                # print cinfo
                ul = json.loads(cinfo['cardsStr'])
                for uj in ul:
                    uv = ul[uj]
                    if 'bankType' in uv and 'resultDetail' in uv:
                        resultdetail = uv['resultDetail']
                        rejson = json.loads(resultdetail)
                        rnum+=1
                        for fea in rejson:
                            # print fea
                            feadic.setdefault(fea,0)
                            feadic[fea]+=1
            except:
                pass
            i+=1
            if i%100==0:
                print i
                time.sleep(0.5)
                # break
        print rnum
        fealist=[]
        for fea in feadic:
            fealist.append([fea,feadic[fea]])
        feadf=pd.DataFrame(fealist,columns=['feaname','num'])
        feadf.to_csv(outfile,index=False)
            # json.dump(rejson,wfp)
        # break

if __name__ == '__main__':
    # phn='18519509565'
    # bankj(phn)
    bankfea()