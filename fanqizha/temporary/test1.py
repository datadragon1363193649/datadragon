# -*- cocoding: utf-8 -*-
import pandas as pd
import sys
reload(sys)
import json

sys.setdefaultencoding('utf-8')
def yuday():
    s1='/Users/ufenqi/Downloads/riskUseridOverdueDays.txt'
    out='/Users/ufenqi/Documents/dataming/base1/data/riskuseridyuday'
    wfp=open(out,'w')
    useridc={}
    with open(s1,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            if linelist[1]=='':
                 linelist[1]='0'
            if linelist[0] in useridc:
                # print linelist,useridc[linelist[0]]
                if float(linelist[1])>float(useridc[linelist[0]][1]):
                    useridc[linelist[0]]=linelist
            else:
                useridc[linelist[0]]=linelist
    userlist=useridc.values()
    num=0
    for ul in userlist:
        print ul
        if float(ul[1])>29:
            num+=1
        wfp.write(','.join(ul)+'\n')
    print num
def trainyuday():
    traindf=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data'
                        '/traindatainner_realtarget.csv')
    s1 = '/Users/ufenqi/Documents/dataming/base1/data/useridtarget3'
    newtarget=[]
    print traindf['1'].value_counts()
    with open(s1,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            newtarget.append([int(linelist[0]),int(linelist[1])])\


            if int(linelist[0]) in traindf['0'].values.tolist():
    # newdf=pd.DataFrame(newtarget,columns=['userid','target'])
                traindf['1'][traindf[traindf['0']==int(linelist[0])].index]=int(linelist[1])
    # traindf.rename(columns={'0': 'userid'}, inplace=True)
    # traindf1=traindf.merge(newdf,on=['userid','userid'],how='left')
    # traindf1['1']=traindf1['target']
    # traindf.rename(columns={'userid': '0'}, inplace=True)
    # del traindf1['target']
    print traindf['1'].value_counts()
    traindf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/'
                    'traindatainner_realtarget3.csv',index=False)
    # print traindf1.head()
    # print traindf['1'].value_counts()
    # print traindf1['target'].value_counts()
def dfhe():
    traindf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data'
                          '/traindatainner_realtarget3.csv')
    traindf1 = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data'
                          '/userfeatureall3_4.csv')
    frames = [traindf, traindf1]
    tdfall = pd.concat(frames)
    print tdfall.__len__()
    tdfall = tdfall.drop(tdfall[tdfall['16'] == -1].index)
    print tdfall.__len__()
    print tdfall['1'].value_counts()
    tdfall.to_csv('/Users/ufenqi/Documents/dataming/base1/data/traindataall3.csv',index=False)
def trainreplace():
    traindf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data'
                          '/traindata_codeinner3_10.csv')
    traindf1 = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data'
                           '/traindataall7.csv')
    # print traindf['1'].value_counts()
    # print traindf.head(20)
    traindf['1']=traindf1['1']
    # print traindf['1'].value_counts()
    # print traindf.head(20)
    # print traindf1.head(20)
    traindf.to_csv('/Users/ufenqi/Documents/dataming/base1/data'
                          '/traindata_codeinner7_10.csv',index=False)
def getfeature():
    s_file='/Users/ufenqi/Documents/dataming/base1/config/featureid'
    s1_file = '/Users/ufenqi/Documents/dataming/base1/config/featurename_onlinetest'
    idlist=[]
    idlist.append('0')
    idlist.append('1')
    feaidlist=[]
    feaidlist.append('0')
    feaidlist.append('1')
    with open(s1_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
    with open(s_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            idlist.append(linelist[0])
    t_file='/Users/ufenqi/Documents/dataming/base1/data/data_1510/trainold.csv'
    tdf=pd.read_csv(t_file)
    t1df=tdf[idlist]
    t1df = t1df.round(10)
    t1df['39u']=1.0*t1df['39']/(t1df['41']+1)
    t1df = t1df.round(10)
    del t1df['39']
    del t1df['41']
    t1df.columns=feaidlist
    t1df.to_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/trainold01.csv',index=False)
def trainday():
    s_file='/Users/ufenqi/Documents/query_result526.csv'
    out_file='/Users/ufenqi/Documents/dataming/base1/data/data_1510/test/phnscore526'
    userinfdic = {}
    wfp=open(out_file,'w')
    with open(s_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            if linelist[1] in userinfdic:
                if float(linelist[-2]) > float(userinfdic[linelist[1]][-2]):
                    userinfdic[linelist[1]] = linelist
            else:
                userinfdic[linelist[1]] = linelist
            # userid = fli[0]
            # phn=fli[1]
            # time=fli[2]
            # wecashscore=fli[3]
            # fraudScore=fli[4]
            # overdays=fli[5]
            # rulenum=fli[6]
    phnlist = userinfdic.keys()
    userlist = userinfdic.values()
    for p in userinfdic:
        wfp.write(','.join(userinfdic[p])+'\n')
def targetupdate():
    tdf=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/codenordeltestinner.csv')
    sfile='/Users/ufenqi/Documents/dataming/base1/data/data_1510/trainoldtarget'

    iddic={}
    with open(sfile,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            # print linelist
            iddic[int(linelist[0])]=float(linelist[1])
    idl=tdf['0'].values.tolist()
    for id1 in idl:
        if id1 in iddic:
            print id1
            tdf['1'][tdf['0'] == id1] = iddic[id1]
    print tdf.dtypes
    tdf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/data_1510/codenordeltestinnerupdate.csv',index=False)
def scorereplsce():
    s_file='/Users/ufenqi/Documents/dataming/base1/data/data_1510/test/feayuqi511'
    s2_file='/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/scorecodenordelphnscore5111.csv'
    out_file='/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/twomodel'
    wfp=open(out_file,'w')
    sdic={}
    with open(s2_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            sdic[linelist[0]]=linelist[1]
    with open(s_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            # if float(linelist[-1])>0.3:
            linelist[-1]=sdic[linelist[1]]
            wfp.write(','.join(linelist)+'\n')
def overduo():
    s_file = '/Users/ufenqi/Documents/dataming/base2/overdue/cui_yu'
    # s2_file = '/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/scorecodenordelphnscore5111.csv'
    out_file = '/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/twomodel'
    wfp = open(out_file, 'w')
    sdic = {}
    tnum=0
    lnum=0
    allnum=0
    aln=0
    cnum=0
    closenum=0
    emptynum=0
    wrnum=0
    wfnum=0
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('^')
            if int(linelist[-1])>59:
                aln += 1
                if '跳票' in linelist[-2] :
                    tnum+=1
                if '老赖' in linelist[-2] :
                    lnum+=1
                if '停机' in linelist[-2]:
                    closenum += 1
                if '空号' in linelist[-2]:
                    emptynum += 1
                if '无人接听' in linelist[-2]:
                    wrnum += 1
                if '无法接通' in linelist[-2]:
                    wfnum += 1
            # if '跳票' in linelist[-1] or '老赖' in linelist[-1] or '承诺还款' in linelist[-1]:
            #     allnum+=1
    print aln
    print '跳票', tnum*1.0/aln
    print '老赖', lnum * 1.0 / aln
    print '停机', closenum * 1.0 / aln
    print '空号', emptynum * 1.0 / aln
    print '无人接听', wrnum * 1.0 / aln
    print '无法接通', wfnum * 1.0 / aln
    # print '老赖或者跳票', allnum * 1.0 / aln
def overduo_liu():
    s_file = '/Users/ufenqi/Documents/dataming/base2/overdue/cui715.csv'
    s2_file='/Users/ufenqi/Documents/dataming/base2/overdue/phn_score7_8'
    # s2_file = '/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/test/scorecodenordelphnscore5111.csv'
    out_file = '/Users/ufenqi/Documents/dataming/base2/overdue/y_phn_7_15'
    wfp = open(out_file, 'w')
    sdic = {}
    with open(s2_file,'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            sdic[linelist[0]]=linelist
    a=[]
    n=0
    with open(s_file, 'r') as fp:
        for line in fp:
            n+=1
            linelist = line.strip().split('^')
            # if int(linelist[-1])>59:
            if '停机' in linelist[-1] or '无人接听' in linelist[-1] or '无法接通' in linelist[-1]:
                if linelist[0] in sdic:
                    wfp.write('\t'.join(sdic[linelist[0]])+'\t'+linelist[-1]+'\n')
                else:
                    a.append(linelist)
    print len(a)
    print n
def mearge():
    sfile='/Users/ufenqi/Documents/dataming/model_yu/data/score_715'
    s2file='/Users/ufenqi/Documents/dataming/model_yu/data/scorecodenordelphn_715_merge_first.csv'
    s3file = '/Users/ufenqi/Documents/dataming/model_yu/data/scorecodenordelphn_715_merge.csv'
    s4file='/Users/ufenqi/Documents/dataming/model_yu/data/cui715.csv'
    # sfile='/Users/ufenqi/Documents/dataming/base1/test/score_715'
    # s2file='/Users/ufenqi/Documents/dataming/base1/test/scorecodenordelphn_715_merge.csv'
    outfile='/Users/ufenqi/Documents/dataming/model_yu/data/score_715_cross_old'
    wfp=open(outfile,'w')
    phndic={}
    fealist=[]
    with open(s2file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            # print linelist
            phndic[linelist[0]]=linelist[1]
    # phn4dic = {}
    # with open(s4file, 'r') as fp:
    #     for line in fp:
    #         linelist = line.strip().split('^')
    #         if u'停机' in linelist[-1] or u'无人接听' in linelist[-1] or u'无法接通' in linelist[-1]:
    #         # print linelist
    #             phn4dic[linelist[0]] = 0
    phn3dic = {}
    with open(s3file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            # print linelist
            phn3dic[linelist[0]] = linelist[1]
    with open(sfile,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            print linelist
            if  linelist[0] in phn3dic:
                # print linelist
                lst=linelist
                lst.append(phndic[linelist[0]])
                lst.append(phn3dic[linelist[0]])
                fealist.append(lst)
                # wfp.write(line.strip()+','+phndic[linelist[0]]+'\n')
    feadf=pd.DataFrame(fealist,columns=['phn', 'wecash', 'rulenum', 'sfscore1','yuqiday','sfscore_first','sfscore_m1'])
    feadf.to_csv(outfile+'.csv',index=False)
def mearge1():
    sfile='/Users/ufenqi/Documents/dataming/model_yu/data/score_merge_715'
    s2file='/Users/ufenqi/Documents/dataming/model_yu/data/query_result_overday.csv'
    s3file = '/Users/ufenqi/Documents/dataming/model_yu/data/scorecodenordelphn_715_merge_1_m1.csv'
    s4file='/Users/ufenqi/Documents/dataming/model_yu/data/cui715.csv'
    # sfile='/Users/ufenqi/Documents/dataming/base1/test/score_715'
    # s2file='/Users/ufenqi/Documents/dataming/base1/test/scorecodenordelphn_715_merge.csv'
    outfile='/Users/ufenqi/Documents/dataming/model_yu/data/score_merge_715_update'
    wfp=open(outfile,'w')
    phndic={}
    fealist=[]
    with open(s2file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            # print linelist
            if float(linelist[1])<0:
                linelist[1]='0'
            phndic[linelist[0]]=linelist[1]
    with open(sfile,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            print linelist
            if linelist[0] in phndic:
                # print linelist
                linelist[4]=phndic[linelist[0]]
                lst=linelist
                # lst.append(phndic[linelist[0]])
                # lst.append(phn3dic[linelist[0]])
                fealist.append(lst)
                wfp.write(','.join(lst) + '\n')
            else:
                lst = linelist
                # lst.append(phndic[linelist[0]])
                # lst.append(phn3dic[linelist[0]])
                fealist.append(lst)
                wfp.write(','.join(lst) + '\n')
    # feadf=pd.DataFrame(fealist,columns=['phn', 'wecash', 'rulenum', 'sfscore1','yuqiday','sfscore_first','sfscore_m1'])
    # feadf.to_csv(outfile+'.csv',index=False)
def replace():
    s1file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/ceiling_old.json'
    s2file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/featurename_online'
    outfile='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/ceiling.json'
    wfp=open(outfile,'w')
    with open(s1file, 'r') as fp:
        c = fp.readline()
        c = json.loads(c)
    outdic={}
    with open(s2file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            if linelist[0] in c:
                print linelist
                print c[linelist[0]]
                outdic[linelist[1]]=c[linelist[0]]
    print len(outdic)
    json.dump(outdic,wfp)
def filtrate():
    s1file='/Users/ufenqi/Documents/dataming/bank/data/score_730_915_pass'
    # s2file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/featurename_online'
    outfile='/Users/ufenqi/Documents/dataming/bank/data/score_730_910_fu'
    wfp=open(outfile,'w')
    out1file = '/Users/ufenqi/Documents/dataming/bank/data/score_910_zheng'
    wfp1 = open(out1file, 'w')
    endtime='2017-09-10 00:00:00'
    num=0
    with open(s1file, 'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            realtime=linelist[1].strip('"')
            if realtime<endtime:
                # print realtime
                if float(linelist[3])>29:
                    wfp.write(linelist[0]+",0"+'\n')
                else:
                    wfp1.write(linelist[0] + ",1" + '\n')

    # with open(s2file,'r') as fp:
    #     for line in fp:
    #         linelist=line.strip().split(',')
    #         if linelist[0] in c:
    #             print linelist
    #             print c[linelist[0]]
    #             outdic[linelist[1]]=c[linelist[0]]
    # print len(outdic)
    # json.dump(outdic,wfp)

def filtrate_day():
    s1file='/Users/ufenqi/Documents/dataming/bank/data/score_730_915'
    # s2file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/featurename_online'
    outfile='/Users/ufenqi/Documents/dataming/tongdun/data/score_911'
    wfp=open(outfile,'w')
    endtime='2017-09-11'
    num=0
    with open(s1file, 'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            realtime=linelist[1].strip('"')
            if endtime in realtime:
                    wfp.write(line.strip()+'\n')

if __name__ == '__main__':
    # yuday()
    # trainyuday()
    # dfhe()
    # trainreplace()
    # trainday()
    # targetupdate()
    # scorereplsce()
    # overduo_liu()
    # mearge1()
    # replace()
    # filtrate()
    filtrate_day()