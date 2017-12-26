# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

def trainnum():
    # 原始数据
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfenxiang_code.csv'
    out_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/tainnumbercnt'
    wfp = open(out_file, 'w')
    with open(f_file, 'r') as fp:
        i1=0
        for line in fp:
            if i1==0:
                i1+=1
                continue
            fea_num={}
            fea_num.setdefault('0', 0)
            fea_num.setdefault('1', 0)
            fea_num.setdefault('2', 0)
            fea_num.setdefault('3', 0)
            fea_num.setdefault('4', 0)
            fea_num.setdefault('5', 0)
            fea_num.setdefault('6', 0)
            linelist = line.strip().split(',')
            linenew=linelist[8:]
            for ln in linenew:
                if ln not  in fea_num:
                    continue
                fea_num[ln]+=1
            wfp.write(str(int(float(linelist[0])))+'\t'+str(fea_num['0'])+'\t'+str(fea_num['1'])+'\t'+str(fea_num['2'])+'\t'+str(fea_num['3'])+'\t'+str(fea_num['4'])+'\t'+str(fea_num['5'])+'\t'+str(fea_num['6'])+'\n')
    wfp.close()
    traindf=pd.read_csv(f_file)
    ff =[]
    with open(out_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    columnl=['userid','n0','n1','n2','n3','n4','n5','n6']
    numdf = pd.DataFrame(ff,columns=columnl)
    traindf.rename(columns={'0':'userid'}, inplace = True)
    # col
    for col in columnl:
        numdf[col]=numdf[col].astype('int')
    traincol = traindf.columns
    for col in traincol:
        traindf[col] = traindf[col].astype('int')
    train=pd.merge(traindf,numdf,on=['userid'],how='left')
    # traindf.join(numdf,on=['userid','userid'])
    train.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/tainnumbercnt.csv',index=False)
def testnum():
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizha/testfenxiang_code.csv'
    out_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizha/testnumbercnt'
    wfp = open(out_file, 'w')
    with open(f_file, 'r') as fp:
        i1 = 0
        for line in fp:
            if i1 == 0:
                i1 += 1
                continue
            fea_num = {}
            fea_num.setdefault('0', 0)
            fea_num.setdefault('1', 0)
            fea_num.setdefault('2', 0)
            fea_num.setdefault('3', 0)
            fea_num.setdefault('4', 0)
            fea_num.setdefault('5', 0)
            fea_num.setdefault('6', 0)
            linelist = line.strip().split(',')
            linenew = linelist[7:]
            for ln in linenew:
                if ln not in fea_num:
                    continue
                fea_num[ln] += 1
            wfp.write(str(int(float(linelist[0]))) + '\t' + str(fea_num['0']) + '\t' + str(fea_num['1']) + '\t' + str(
                fea_num['2']) + '\t' + str(fea_num['3']) + '\t' + str(fea_num['4']) + '\t' + str(
                fea_num['5']) + '\t' + str(fea_num['6']) + '\n')
    wfp.close()
    traindf = pd.read_csv(f_file)
    ff = []
    with open(out_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    columnl = ['userid', 'n0', 'n1', 'n2', 'n3', 'n4', 'n5', 'n6']
    numdf = pd.DataFrame(ff, columns=columnl)
    traindf.rename(columns={'0': 'userid'}, inplace=True)
    # col
    for col in columnl:
        numdf[col] = numdf[col].astype('int')
    print numdf.dtypes
    traincol=traindf.columns
    for col in traincol:
        traindf[col]=traindf[col].astype('int')
    print traindf.dtypes
    train = pd.merge(traindf, numdf, on=['userid'], how='left')
    print train.dtypes
    # traindf.join(numdf,on=['userid','userid'])
    train.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizha/testnumbercnt.csv', index=False)
if __name__ == '__main__':
    trainnum()