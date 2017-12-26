#! /usr/bin/env python
# -*- encoding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
def t1():
    sfile='/Users/ufenqi/Documents/dataming/bank/config/featurename_online'
    outfile='/Users/ufenqi/Documents/dataming/bank/config/featurename_online1'
    # # s2file='/Users/ufenqi/Documents/dataming/bank/config/featuretest'
    # wfp=open(outfile,'w')
    realdic={}
    # uid=133
    # with open(sfile,'r') as fp:
    #     for line in fp:
    #         linelist=line.strip().split(',')
    #         if linelist[3]=='class':
    #             realdic[linelist[0]]=0
    #         wfp.write(str(uid)+','+','.join(linelist[1:])+'\n')
    #         uid+=1
    # print len(realdic)
    # testdic={}
    # with open(s2file,'r') as fp:
    #     for line in fp:
    #         linelist=line.strip().split(',')
    #         if linelist[1] in testdic:
    #             print linelist
    #         testdic[linelist[1]]=linelist[0]
    #         feaname=linelist[1].strip().split('.')
    #         if feaname[1] in realdic:
    #             if realdic[feaname[1]] != linelist[0]:
    #                 print realdic[feaname[1]],feaname[1],linelist[0]
    #         else:
    #             print linelist
    # print len(testdic)
    import pandas as pd
    tdf=pd.read_csv('/Users/ufenqi/Documents/dataming/bank/data/train_phn_730_9101.csv')
    columnslist=tdf.columns
    lent = 0.85 * len(tdf)
    collist=[]
    # for i in range(0,len(columnslist)):
    #     for j in range(0,len(columnslist)):
    #         if columnslist[i]==columnslist[j]:
    #             continue
    #     #     if tdf[columnslist[i]].corr(tdf[columnslist[j]])==1:
    #     #         print i,j,tdf[columnslist[i]].corr(tdf[columnslist[j]])
    #         tvci = tdf[columnslist[i]].value_counts()
    #         tvcj = tdf[columnslist[j]].value_counts()
    #         if tvci[tvci.index[0]]==tvcj[tvcj.index[0]] and tvci.index[0]==tvcj.index[0] and len(tvci)==len(tvcj) and len(tvci)>=2:
    #             tf=0
    #             for k in range(0,len(tvci)):
    #                 if tvci[tvci.index[k]] != tvcj[tvcj.index[k]] or tvci.index[k] != tvcj.index[k]:
    #                     tf=1
    #             # if tvci[tvci.index[0]]==tvcj[tvcj.index[0]] and tvci.index[0]==tvcj.index[0]
    #             if tf==0:
    #                 collist.append(1)
    #                 print columnslist[i],columnslist[j]
    #                 print tdf[columnslist[i]].value_counts().head(5)
    #                 print '*********************'
    #                 print tdf[columnslist[j]].value_counts().head(5)
    #                 print '^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'
    # print len(collist)

    # for col in realdic:
    #     print col
    #     print tdf[col].value_counts()
    # print len(realdic)
    for col in columnslist[1:]:
        tvc = tdf[col].value_counts()
        if tvc[tvc.index[0]] > lent:
            print col
            collist.append(col)
            print tdf[col].value_counts().head(5)
            print '*********************'
    print len(collist)
    # with open(sfile,'r') as fp:
    #     for line in fp:
    #         linelist=line.strip().split(',')
    #         if linelist[0] in collist:
    #             continue
    #         wfp.write(line.strip()+'\n')
def t2():
    sfile = '/Users/ufenqi/Documents/dataming/bank/config/featurename_online1'
    outfile = '/Users/ufenqi/Documents/dataming/bank/config/featurename_online2'
    wfp = open(outfile, 'w')
    classlist=['151','154','159','158','134','139','138','395','185','188',
               '408','201','140','141','218','162','412','396']
    num=0
    with open(sfile,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            if linelist[-1]=='class':
                num+=1
                if linelist[0] in classlist:
                    wfp.write(line.strip()+'\n')
            else:
                wfp.write(line.strip() + '\n')
    print num
if __name__ == '__main__':
    t1()