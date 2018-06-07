# -*- cocoding: utf-8 -*-
import offline_db_conf as conf
import pandas as pd
# stacking训练数据合并
def train():
    cdf1=pd.read_csv(conf.data_path+'call/scoretrain_call_0.csv',names=['phn','call'])
    cdf2=pd.read_csv(conf.data_path+'call/scoretrain_call_1.csv',names=['phn','call'])
    cdf3=pd.read_csv(conf.data_path+'call/scoretrain_call_2.csv',names=['phn','call'])
    cdf4=pd.read_csv(conf.data_path+'call/scoretrain_call_3.csv',names=['phn','call'])
    calldf=pd.concat([cdf1,cdf2,cdf3,cdf4])
    calldf.to_csv(conf.data_path+'call_train.csv',index=False)
    tdf1=pd.read_csv(conf.data_path+'tongdun/scoretrain_tongdun_0.csv',names=['phn','tongdun'])
    tdf2=pd.read_csv(conf.data_path+'tongdun/scoretrain_tongdun_1.csv',names=['phn','tongdun'])
    tdf3=pd.read_csv(conf.data_path+'tongdun/scoretrain_tongdun_2.csv',names=['phn','tongdun'])
    tdf4=pd.read_csv(conf.data_path+'tongdun/scoretrain_tongdun_3.csv',names=['phn','tongdun'])
    tongdundf=pd.concat([tdf1,tdf2,tdf3,tdf4])
    tongdundf.to_csv(conf.data_path+'tongdun_train.csv',index=False)
    adf1=pd.read_csv(conf.data_path+'app/scoretrain_app_0.csv',names=['phn','app'])
    adf2=pd.read_csv(conf.data_path+'app/scoretrain_app_1.csv',names=['phn','app'])
    adf3=pd.read_csv(conf.data_path+'app/scoretrain_app_2.csv',names=['phn','app'])
    adf4=pd.read_csv(conf.data_path+'app/scoretrain_app_3.csv',names=['phn','app'])
    appdf=pd.concat([adf1,adf2,adf3,adf4])
    appdf.to_csv(conf.data_path+'app_train.csv',index=False)
    traindf=pd.read_csv(conf.data_path+'call/codenordelindclasstrain_uid_target_all_call.csv')
    targetdf=traindf[['0','1']]
    targetdf.columns=['phn','target']
    integrationdf=targetdf.merge(calldf,on=['phn','phn'])
    integrationdf=integrationdf.merge(tongdundf,on=['phn','phn'])
    integrationdf=integrationdf.merge(appdf,on=['phn','phn'])
    print integrationdf.head()
    print len(integrationdf)
    integrationdf.columns=['0','1','call','tongdun','app']
    integrationdf.to_csv(conf.data_path+'train_integration.csv',index=False)
# stacking测试数据合并
def test():
    datestr='90101'
    cdf1 = pd.read_csv(conf.data_path + 'call/scorecodenordelclasstest_data_'+datestr+'_s0.csv', names=['phn', 'call'])
    cdf2 = pd.read_csv(conf.data_path + 'call/scorecodenordelclasstest_data_'+datestr+'_s1.csv', names=['phn', 'call2'])
    cdf3 = pd.read_csv(conf.data_path + 'call/scorecodenordelclasstest_data_'+datestr+'_s2.csv', names=['phn', 'call3'])
    cdf4 = pd.read_csv(conf.data_path + 'call/scorecodenordelclasstest_data_'+datestr+'_s3.csv', names=['phn', 'call4'])
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
    tdf1 = pd.read_csv(conf.data_path + 'tongdun/scorecodenordelclasstest_data_'+datestr+'_s0.csv', names=['phn', 'tongdun'])
    tdf2 = pd.read_csv(conf.data_path + 'tongdun/scorecodenordelclasstest_data_'+datestr+'_s1.csv', names=['phn', 'tongdun2'])
    tdf3 = pd.read_csv(conf.data_path + 'tongdun/scorecodenordelclasstest_data_'+datestr+'_s2.csv', names=['phn', 'tongdun3'])
    tdf4 = pd.read_csv(conf.data_path + 'tongdun/scorecodenordelclasstest_data_'+datestr+'_s3.csv', names=['phn', 'tongdun4'])
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
    adf1 = pd.read_csv(conf.data_path + 'app/scorecodenordelclasstest_data_'+datestr+'_s0.csv', names=['phn', 'app'])
    adf2 = pd.read_csv(conf.data_path + 'app/scorecodenordelclasstest_data_'+datestr+'_s1.csv', names=['phn', 'app2'])
    adf3 = pd.read_csv(conf.data_path + 'app/scorecodenordelclasstest_data_'+datestr+'_s2.csv', names=['phn', 'app3'])
    adf4 = pd.read_csv(conf.data_path + 'app/scorecodenordelclasstest_data_'+datestr+'_s3.csv', names=['phn', 'app4'])
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
    integrationdf.to_csv(conf.data_path + 'test_integration_'+datestr+'.csv', index=False)
# 袋装法
def score_avc():
    s1file=conf.data_path+'score_call_9010'
    s2file = conf.data_path+'score_tongdun_9010'
    # s3file = conf.data_path+'app/score9010_app_g'
    outfile=conf.data_path+'score_bagging_9010'
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
            if linelist[0] in scoredic:
                scoredic[linelist[0]]+=float(linelist[-1])
    # with open(s3file, 'r') as fp:
    #     for line in fp:
    #         linelist = line.strip().split(',')
    #         if linelist[0] in scoredic:
    #             scoredic[linelist[0]] += float(linelist[-1])
    for phn in scoredic:
        score=scoredic[phn]/2
        list=s1dic[phn]
        list.append(str(score))
        wfp.write(','.join(list)+'\n')
    # print scoredic
    # print s1dic
# 训练集分割
def train_use_data():
    tdf=pd.read_csv('/Users/ufenqi/Documents/dataming/integration_new/data/'
                    'app/codenordelindclasstrain_uid_target_all_app1.csv')
    sfile0='/Users/ufenqi/Documents/dataming/integration_new/data/train_uid_sample0'
    sfile1 = '/Users/ufenqi/Documents/dataming/integration_new/data/train_uid_sample1'
    sfile2 = '/Users/ufenqi/Documents/dataming/integration_new/data/train_uid_sample2'
    sfile3 = '/Users/ufenqi/Documents/dataming/integration_new/data/train_uid_sample3'
    fname=[sfile0,sfile1,sfile2,sfile3]
    ti=0
    for fn in fname:
        uidlist=[]
        with open(fn,'r') as fp:
            for line in fp:
                uidlist.append(int(line.strip()))
        testdf=tdf[tdf['0'].isin(uidlist)]
        # print testdf.__len__()
        # print testdf.head()
        traindf=tdf.drop(testdf.index)
        testdf.to_csv('/Users/ufenqi/Documents/dataming/integration_new/data/'
                    'app/train_app_'+str(ti)+'.csv',index=False)
        traindf.to_csv('/Users/ufenqi/Documents/dataming/integration_new/data/'
                      'app/train_app_tr_' + str(ti) + '.csv',index=False)
        ti+=1
        # print traindf.__len__()
        # print traindf.head()
# score_card train
def score_card_train():
    cdf1=pd.read_csv(conf.data_path+'call/scorecodenordelclasstrain_score_call_data.csv',names=['phn','call'])
    tdf1=pd.read_csv(conf.data_path+'tongdun/scorecodenordelclasstrain_score_tongdun_data.csv',names=['phn','tongdun'])
    bdf1=pd.read_csv(conf.data_path+'bank/scorebank.csv',names=['phn','bank'])
    traindf=pd.read_csv(conf.data_path+'call/codenordelclasstrain_score_call_data.csv')
    targetdf=traindf[['0','1']]
    targetdf.columns=['phn','target']
    integrationdf=targetdf.merge(cdf1,on=['phn','phn'])
    integrationdf=integrationdf.merge(tdf1,on=['phn','phn'])
    integrationdf=integrationdf.merge(bdf1,on=['phn','phn'])
    print integrationdf.head()
    print len(integrationdf)
    integrationdf.columns=['0','1','call','tongdun','bank']
    integrationdf.to_csv(conf.data_path+'train_score_card.csv',index=False)
# score_card test
def score_card_test():
    datestr='21'
    cdf1 = pd.read_csv(conf.data_path + 'call/scorecodenordelclasstest_data'+datestr+'.csv', names=['phn', 'call'])
    tdf1 = pd.read_csv(conf.data_path + 'tongdun/scorecodenordelclasstest_data'+datestr+'.csv', names=['phn', 'tongdun'])
    bdf1 = pd.read_csv(conf.data_path + 'bank/score_bank_test.csv', names=['phn', 'bank'])
    integrationdf = cdf1.merge(tdf1, on=['phn', 'phn'])
    integrationdf = integrationdf.merge(bdf1, on=['phn', 'phn'],how='right')
    print integrationdf.head()
    print len(integrationdf)
    integrationdf.columns = ['0', 'call', 'tongdun', 'bank']
    integrationdf.to_csv(conf.data_path + 'test_score_test_90'+datestr+'.csv', index=False)
def lin():
    cdf1 = pd.read_csv(conf.data_path + 'call/scorecodenordelclasstest_data21.csv', names=['phn', 'call'])
    tdf1 = pd.read_csv(conf.data_path + 'tongdun/scorecodenordelclasstest_data21.csv',
                       names=['phn', 'tongdun'])
    bdf1 = pd.read_csv(conf.data_path + 'bank/score_bank_test.csv', names=['phn', 'bank'])
    scoredf=pd.read_csv(conf.data_path+'scoretest_score_test_9021.csv',names=['phn','card'])
    integrationdf = cdf1.merge(tdf1, on=['phn', 'phn'])
    integrationdf['baging']=(integrationdf['call']+integrationdf['tongdun'])/2
    integrationdf = integrationdf.merge(bdf1, on=['phn', 'phn'], how='right')
    integrationdf=integrationdf.merge(scoredf,on=['phn', 'phn'],how='right')
    print integrationdf.head()
    print len(integrationdf)
    integrationdf.columns = ['0', 'call', 'tongdun','baging', 'bank','card']
    integrationdf.to_csv(conf.data_path + 'all_score_data.csv', index=False)
if __name__ == '__main__':
    # test()
    # train()
    # score_avc()
    # train_use_data()
    # score_card_train()
    # score_card_test()
    lin()






