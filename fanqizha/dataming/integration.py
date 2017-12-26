# -*- cocoding: utf-8 -*-
import offline_db_conf as conf
import pandas as pd
def train():
    cdf1=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_call_s1.csv',names=['phn','call'])
    cdf2=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_call_s2.csv',names=['phn','call'])
    cdf3=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_call_s3.csv',names=['phn','call'])
    cdf4=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_call_s4.csv',names=['phn','call'])
    calldf=pd.concat([cdf1,cdf2,cdf3,cdf4])
    calldf.to_csv(conf.data_path+'call_train.csv',index=False)
    tdf1=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_tongdun_s1.csv',names=['phn','tongdun'])
    tdf2=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_tongdun_s2.csv',names=['phn','tongdun'])
    tdf3=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_tongdun_s3.csv',names=['phn','tongdun'])
    tdf4=pd.read_csv(conf.data_path+'scorecodenordelindtrain_phn_730_9101_tongdun_s4.csv',names=['phn','tongdun'])
    tongdundf=pd.concat([tdf1,tdf2,tdf3,tdf4])
    tongdundf.to_csv(conf.data_path+'tongdun_train.csv',index=False)
    adf1=pd.read_csv(conf.data_path+'scorewoecodenordelindclasstrain_phn_730_9101_app_s1.csv',names=['phn','app'])
    adf2=pd.read_csv(conf.data_path+'scorewoecodenordelindclasstrain_phn_730_9101_app_s2.csv',names=['phn','app'])
    adf3=pd.read_csv(conf.data_path+'scorewoecodenordelindclasstrain_phn_730_9101_app_s3.csv',names=['phn','app'])
    adf4=pd.read_csv(conf.data_path+'scorewoecodenordelindclasstrain_phn_730_9101_app_s4.csv',names=['phn','app'])
    appdf=pd.concat([adf1,adf2,adf3,adf4])
    appdf.to_csv(conf.data_path+'app_train.csv',index=False)
    traindf=pd.read_csv('/Users/ufenqi/Documents/dataming/app/data/codenordelindclasstrain_phn_730_9101.csv')
    targetdf=traindf[['0','1']]
    targetdf.columns=['phn','target']
    integrationdf=targetdf.merge(calldf,on=['phn','phn'])
    integrationdf=integrationdf.merge(tongdundf,on=['phn','phn'])
    integrationdf=integrationdf.merge(appdf,on=['phn','phn'])
    print integrationdf.head()
    print len(integrationdf)
    integrationdf.columns=['0','1','call','tongdun','app']
    integrationdf.to_csv(conf.data_path+'train_integration.csv',index=False)
def test():

    cdf1 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_call_s1.csv', names=['phn', 'call'])
    cdf2 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_call_s2.csv', names=['phn', 'call2'])
    cdf3 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_call_s3.csv', names=['phn', 'call3'])
    cdf4 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_call_s4.csv', names=['phn', 'call4'])
    calldf = cdf1.merge(cdf2, on=['phn', 'phn'])
    calldf = calldf.merge(cdf3, on=['phn', 'phn'])
    calldf = calldf.merge(cdf4, on=['phn', 'phn'])
    print calldf.head()
    print len(calldf)
    calldf2=calldf.add(calldf['call2'],axis=0)
    calldf2 = calldf2.add(calldf['call3'], axis=0)
    calldf2 = calldf2.add(calldf['call4'], axis=0)
    calldf2=calldf2[['phn','call']]
    calldf2['call']=calldf2['call']/4
    calldf2['phn']=calldf['phn']
    print calldf2.head()
    # calldf.to_csv(conf.data_path + 'call_train.csv', index=False)
    tdf1 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_tongdun_s1.csv', names=['phn', 'tongdun'])
    tdf2 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_tongdun_s2.csv', names=['phn', 'tongdun2'])
    tdf3 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_tongdun_s3.csv', names=['phn', 'tongdun3'])
    tdf4 = pd.read_csv(conf.data_path + 'scorecodenordelscore_9111_tongdun_s4.csv', names=['phn', 'tongdun4'])
    # tongdundf = pd.concat([tdf1, tdf2, tdf3, tdf4])
    tongdundf = tdf1.merge(tdf2, on=['phn', 'phn'])
    tongdundf = tongdundf.merge(tdf3, on=['phn', 'phn'])
    tongdundf = tongdundf.merge(tdf4, on=['phn', 'phn'])
    print tongdundf.head()
    print len(tongdundf)
    tongdundf2 = tongdundf.add(tongdundf['tongdun2'], axis=0)
    tongdundf2 = tongdundf2.add(tongdundf['tongdun3'], axis=0)
    tongdundf2 = tongdundf2.add(tongdundf['tongdun4'], axis=0)
    tongdundf2 = tongdundf2[['phn', 'tongdun']]
    tongdundf2['tongdun'] = tongdundf2['tongdun'] / 4
    tongdundf2['phn'] = tongdundf['phn']
    # tongdundf.to_csv(conf.data_path + 'tongdun_train.csv', index=False)
    adf1 = pd.read_csv(conf.data_path + 'scorewoecodenordelclassscore_9111_app_s1.csv', names=['phn', 'app'])
    adf2 = pd.read_csv(conf.data_path + 'scorewoecodenordelclassscore_9111_app_s2.csv', names=['phn', 'app2'])
    adf3 = pd.read_csv(conf.data_path + 'scorewoecodenordelclassscore_9111_app_s3.csv', names=['phn', 'app3'])
    adf4 = pd.read_csv(conf.data_path + 'scorewoecodenordelclassscore_9111_app_s4.csv', names=['phn', 'app4'])
    # appdf = pd.concat([adf1, adf2, adf3, adf4])
    # appdf.to_csv(conf.data_path + 'app_train.csv', index=False)
    appdf = adf1.merge(adf2, on=['phn', 'phn'])
    appdf = appdf.merge(adf3, on=['phn', 'phn'])
    appdf = appdf.merge(adf4, on=['phn', 'phn'])
    print appdf.head()
    print len(appdf)
    appdf2 = appdf.add(appdf['app2'], axis=0)
    appdf2 = appdf2.add(appdf['app3'], axis=0)
    appdf2 = appdf2.add(appdf['app4'], axis=0)
    appdf2 = appdf2[['phn', 'app']]
    appdf2['app'] = appdf2['app'] / 4
    appdf2['phn'] = appdf['phn']
    integrationdf = calldf2.merge(tongdundf2, on=['phn', 'phn'])
    integrationdf = integrationdf.merge(appdf2, on=['phn', 'phn'])
    print integrationdf.head()
    print len(integrationdf)
    integrationdf.columns = ['0', 'call', 'tongdun', 'app']
    integrationdf.to_csv(conf.data_path + 'test_integration_911.csv', index=False)
def score_avc():
    s1file='/Users/ufenqi/Documents/dataming/call/data/score_call_911_use'
    s2file = '/Users/ufenqi/Documents/dataming/tongdun/data/score_tongdun_911_use'
    s3file = '/Users/ufenqi/Documents/dataming/app/data/score_app_911_use'
    outfile='/Users/ufenqi/Documents/dataming/integration/data/score_bagging_911'
    wfp=open(outfile,'w')
    scoredic={}
    s1dic={}
    with open(s1file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            scoredic[linelist[0]]=float(linelist[-1])
            s1dic[linelist[0]]=linelist[:-1]
    with open(s2file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            scoredic[linelist[0]]+=float(linelist[-1])
    with open(s3file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            scoredic[linelist[0]] += float(linelist[-1])
    for phn in scoredic:
        score=scoredic[phn]/3
        list=s1dic[phn]
        list.append(str(score))
        wfp.write(','.join(list)+'\n')
    # print scoredic
    # print s1dic
if __name__ == '__main__':
    # test()
    # train()
    score_avc()








