# -*- cocoding: utf-8 -*-
import pandas as pd
import sys
reload(sys)
import json
sfile='/Users/ufenqi/Documents/dataming/taobao/data/query_result_taobaotest'
s2file='/Users/ufenqi/Documents/dataming/taobao/data/taobao_user_train'
out1file='/Users/ufenqi/Documents/dataming/taobao/data/taobao_train_data'
out2file='/Users/ufenqi/Documents/dataming/taobao/data/taobao_test_data'
wfp1=open(out1file,'w')
wfp2=open(out2file,'w')
userdic={}
phndic={}
with open(s2file,'r') as fp:
    for line in fp:
        linelist=line.split('\t')
        # print linelist[0]
        phndic[linelist[0]]=line
phnrealdic={}
with open(sfile,'r') as fp:
    for line in fp:
        linelist=line.split(',')
        if linelist[1]!='NULL' and linelist[0] in phndic:
            # print linelist[1]
            datal=linelist[1].strip('"').split('-')
            datas=datal[0]
            if len(datal[1])<2:
                datas=datas+'-0'+datal[1]
            else:
                datas = datas+'-'+ datal[1]
            if len(datal[2])<2:
                datas = datas + '-0' + datal[2]
            else:
                datas = datas +'-'+ datal[2]
            # print datas
            # datas="2017-09-25"
            if datas <'2017-09-30':
                phnrealdic[linelist[0]]=phndic[linelist[0]]
            if datas >='2017-08-20' and datas <'2017-09-30':
                # print datas
                userdic.setdefault(linelist[0],float(linelist[-1]))
                if float(linelist[-1])>userdic[linelist[0]]:
                    userdic[linelist[0]]=float(linelist[-1])
        # break

num=0
for user in userdic:
    if userdic[user]>6:
        num+=1
        # print user,userdic[user]
print 'phnrealdic',len(phnrealdic)
print 'phndic',len(phndic)
print 'userdic',len(userdic)
print 'yu',num
for phn in phnrealdic:
    if phn in userdic:
        wfp2.write(phnrealdic[phn])
    else:
        wfp1.write(phnrealdic[phn])
