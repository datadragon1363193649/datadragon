# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
def percConvert(ser):
    return ser/float(ser[-1])
def train():
    def norandcode():
        # 原始数据
        f_file = '/Users/ufenqi/Documents/dataming/base1/data/traindfnorinner3.csv'
        # test_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturenor.csv'
        fenxiang1out_file='/Users/ufenqi/Documents/dataming/base1/config/binning.json'
        numbercnt_file='/Users/ufenqi/Documents/dataming/base1/config/numbercnt10'
        # nor_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurecallnor'
        # norfealist=[]
        # with open(nor_file, 'r') as fp:
        #     for line in fp:
        #         norfealist = line.strip().split(',')

        wfp = open(fenxiang1out_file, 'w')
        nwfp = open(numbercnt_file, 'w')
        fdf = pd.read_csv(f_file)
        fdf=fdf.round(10)
        # testdf = pd.read_csv(test_file)
        # testdf=testdf.round(10)
        columnslist=fdf.columns.tolist()
        columnslist.remove('0')
        columnslist.remove('1')
        columnslist.remove('2')
        columnslist.remove('63')

        # columnslist=norfealist
        # columnslist.remove('0')
        # # columnslist.remove('1')
        # columnslist.remove('2')
        # columnslist.remove('63')

        numcol=[]
        numfen=[]
        nonumcol=[]
        nonumcol.append('0')
        nonumcol.append('1')
        nonumcol.append('2')
        nonumcol.append('63')
        nonfen=[]
        nonfen.append('0')
        nonfen.append('1')
        nonfen.append('2')
        nonfen.append('63')
        dflen=fdf.__len__()
        binningdic={}
        for col in columnslist:
            fdf[col],arrylist=pd.qcut(fdf[col],10,labels=False,retbins=True,duplicates='drop')
            # if len(fdf[col].unique()) < 2:
            #     del fdf[col]
            #     print col
            #     continue
            # colmaxlen = fdf[col].value_counts().values.tolist()[0]
            # if colmaxlen > (dflen * 0.8):
            #     del fdf[col]
            #     continue
            strlist=[]
            for i in range(0,len(arrylist)) :
                arrylist[i]=round(arrylist[i],10)
                strlist.append(round(float(arrylist[i]),10))
            # print strlist
            nonumcol.append(col)
            nonfen.append(col + 'fen10')
            # if len(arrylist)==11:
            #     numcol.append(col)
            #     numfen.append(col+'fen')
            # else:
            #     nonumcol.append(col)
            #     nonfen.append(col+'fen')
            binningdic[col]=strlist
            # wfp.write(col+'\t'+'\t'.join(strlist)+'\n')
            # testvll = list(set(testdf[col].values.tolist()))
            dfl = []
            # for tvl in testvll:
            #     if tvl > arrylist[-2]:
            #         dfl.append(len(arrylist) - 2)
            #         continue
            #     for j in range(1, len(arrylist)):
            #         if tvl <= arrylist[j]:
            #             dfl.append(j - 1)
            #             print 'cal',col,'tvl',tvl,'arrylist',arrylist[j]
            #             break
            # if len(dfl) != 0:
            #     testdf[col].replace(testvll, dfl, inplace=True)
        json.dump(binningdic,wfp)
        nwfp.write(str(len(nonumcol)))
        nwfp.close()
        # nonumcol+=numcol
        ndf=fdf[nonumcol]
        # tndf=testdf[nonumcol]
        nonfen+=numfen
        ndf.columns=nonfen
        # tndf.columns=nonfen
        ndf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/traindata_codeinner3_10.csv',index=False)
        # tndf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturecode.csv',index=False)
        wfp.close()
        # tnordf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnorinner.csv')
        # tcodedf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_codeinner_10.csv')
        # del tcodedf['1']
        # del tcodedf['2']
        # del tcodedf['63']
        # trainnorandcode=pd.merge(tnordf,tcodedf,on=['0','0'],how='left')
        #
        # testnordf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdfnor4-1.csv')
        # testcodedf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code4-1_10.csv')
        # del testcodedf['1']
        # del testcodedf['2']
        # del testcodedf['63']
        # testnorandcode=pd.merge(testnordf,testcodedf,on=['0','0'],how='left')
        #
        # trainnorandcode.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodeinner_10.csv',index=False)
        # testnorandcode.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode4-_10.csv',index=False)
    norandcode()
def test():
    def norandcode():
        # 原始数据
        f_file = '/Users/ufenqi/Documents/dataming/base1/data/traindfnorinner3.csv'
        test_file = '/Users/ufenqi/Documents/dataming/base1/data/userfeaturenor_501.csv'
        fenxiang1out_file = '/Users/ufenqi/Documents/dataming/base1/config/binning.json'
        numbercnt_file = '/Users/ufenqi/Documents/dataming/base1/config/numbercnt10'
        nor_file = '/Users/ufenqi/Documents/dataming/base1/config/featurecallnor1'
        code_file='/Users/ufenqi/Documents/dataming/base1/config/featurecallcode'
        norfealist = []
        noridlist=[]
        nordic={}
        wfpc = open(code_file, 'w')
        with open(nor_file, 'r') as fp:
            for line in fp:
                norfealist = line.strip().split(',')
                noridlist.append(norfealist[0])
                nordic[norfealist[0]]=norfealist[1]
                if norfealist[0]=='2'or norfealist[0]=='63':
                    wfpc.write(norfealist[0] + ',' + norfealist[1] + '\n')
                else:
                    wfpc.write(norfealist[0]+'fen10'+','+norfealist[1]+'\n')
                # codedic[norfealist[0]+'fen10']=norfealist[1]
        wfp = open(fenxiang1out_file, 'w')
        nwfp = open(numbercnt_file, 'w')

        fdf = pd.read_csv(f_file)
        fdf = fdf.round(10)
        testdf = pd.read_csv(test_file)
        testdf = testdf.round(10)
        columnslist = fdf.columns.tolist()
        columnslist.remove('0')
        # columnslist.remove('1')
        columnslist.remove('2')
        columnslist.remove('63')

        columnslist = noridlist
        columnslist.remove('0')
        # columnslist.remove('1')
        columnslist.remove('2')
        columnslist.remove('63')

        numcol = []
        numfen = []
        nonumcol = []
        nonumcol.append('0')
        # nonumcol.append('1')
        nonumcol.append('2')
        nonumcol.append('63')
        nonfen = []
        nonfen.append('0')
        # nonfen.append('1')
        nonfen.append('2')
        nonfen.append('63')
        dflen = fdf.__len__()
        binningdic = {}
        for col in columnslist:
            fdf[col],arrylist=pd.qcut(fdf[col],10,labels=False,retbins=True,duplicates='drop')
            # if len(fdf[col].unique()) < 2:
            #     del fdf[col]
            #     print col
            #     continue
            # colmaxlen = fdf[col].value_counts().values.tolist()[0]
            # if colmaxlen > (dflen * 0.8):
            #     del fdf[col]
            #     continue
            strlist=[]
            for i in range(0,len(arrylist)) :
                arrylist[i]=round(arrylist[i],10)
                strlist.append(round(float(arrylist[i]),10))
            # print strlist
            nonumcol.append(col)
            nonfen.append(col + 'fen10')
            # if len(arrylist)==11:
            #     numcol.append(col)
            #     numfen.append(col+'fen')
            # else:
            #     nonumcol.append(col)
            #     nonfen.append(col+'fen')
            binningdic[nordic[col]]=strlist
            # wfp.write(col+'\t'+'\t'.join(strlist)+'\n')
            testvll = list(set(testdf[col].values.tolist()))
            dfl = []
            su=0
            for tvl in testvll:
                if tvl > arrylist[-2]:
                    dfl.append(len(arrylist) - 2)
                    su += 1
                    continue
                bol=0
                for j in range(1, len(arrylist)):
                    if tvl <= arrylist[j]:
                        dfl.append(j - 1)
                        # print 'cal',col,'tvl',tvl,'arrylist',arrylist[j]
                        bol=1
                        break
                # if bol==0:
                    # print 'safafagggdfa',tvl,arrylist
                su+=1
                # print su,len(dfl)
            if len(dfl) != 0:
                testdf[col].replace(testvll, dfl, inplace=True)
        json.dump(binningdic, wfp)
        nwfp.write(str(len(nonumcol)))
        nwfp.close()
        # nonumcol+=numcol
        ndf = fdf[nonumcol]
        tndf = testdf[nonumcol]
        nonfen += numfen
        ndf.columns = nonfen
        tndf.columns = nonfen

        # ndf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_codeinner_10.csv',index=False)
        tndf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeaturecode_501.csv', index=False)
        wfp.close()
        # tnordf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnorinner.csv')
        # tcodedf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_codeinner_10.csv')
        # del tcodedf['1']
        # del tcodedf['2']
        # del tcodedf['63']
        # trainnorandcode=pd.merge(tnordf,tcodedf,on=['0','0'],how='left')
        #
        # testnordf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdfnor4-1.csv')
        # testcodedf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code4-1_10.csv')
        # del testcodedf['1']
        # del testcodedf['2']
        # del testcodedf['63']
        # testnorandcode=pd.merge(testnordf,testcodedf,on=['0','0'],how='left')
        #
        # trainnorandcode.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodeinner_10.csv',index=False)
        # testnorandcode.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode4-_10.csv',index=False)

    norandcode()
def code10():
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnorinner.csv'
    test_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdfnorinner.csv'
    fenxiang1out_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/fenxiang11'
    numbercnt_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/numbercnt11'

    wfp = open(fenxiang1out_file, 'w')
    nwfp = open(numbercnt_file, 'w')
    fdf = pd.read_csv(f_file)
    testdf = pd.read_csv(test_file)
    columnslist = fdf.columns.tolist()
    columnslist.remove('0')
    columnslist.remove('1')
    columnslist.remove('2')
    columnslist.remove('63')
    numcol = []
    numfen = []
    nonumcol = []
    nonumcol.append('0')
    nonumcol.append('1')
    nonumcol.append('2')
    nonumcol.append('63')
    nonfen = []
    nonfen.append('0')
    nonfen.append('1')
    nonfen.append('2')
    nonfen.append('63')
    dflen = fdf.__len__()

    for col in columnslist:
        fdf[col], arrylist = pd.qcut(fdf[col], 10, labels=False, retbins=True, duplicates='drop')
        colmaxlen = fdf[col].value_counts().values.tolist()[0]
        # if colmaxlen > (dflen * 0.8):
        #     del fdf[col]
        #     continue
        strlist = []
        for i in range(0, len(arrylist)):
            strlist.append(str(arrylist[i]))
        if len(arrylist) == 11:
            numcol.append(col)
            numfen.append(col + 'fen1')
        else:
            nonumcol.append(col)
            nonfen.append(col + 'fen1')
        wfp.write(col + '\t' + '\t'.join(strlist) + '\n')
        testvll = list(set(testdf[col].values.tolist()))
        dfl = []
        for tvl in testvll:
            if tvl > arrylist[-2]:
                dfl.append(len(arrylist) - 2)
                continue
            for j in range(1, len(arrylist)):
                if tvl <= arrylist[j]:
                    dfl.append(j - 1)
                    break
        if len(dfl) != 0:
            testdf[col].replace(testvll, dfl, inplace=True)
    nwfp.write(str(len(nonumcol)))
    nwfp.close()
    nonumcol += numcol
    ndf = fdf[nonumcol]
    tndf = testdf[nonumcol]
    nonfen += numfen
    ndf.columns = nonfen
    tndf.columns = nonfen

    ndf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_codeinner_10.csv', index=False)
    tndf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_codeinner_10.csv', index=False)
    wfp.close()

    # traincode1=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_code1.csv')
    # testcode1 = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code1.csv')
    #
    # traincode2 = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
    # testcode2 = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode.csv')
    #
    # del traincode1['1']
    # del traincode1['2']
    # del traincode1['63']
    # traincode3 = pd.merge(traincode2, traincode1, on=['0', '0'], how='left')
    # del testcode1['1']
    # del testcode1['2']
    # del testcode1['63']
    # testcode3 = pd.merge(testcode2, testcode1, on=['0', '0'], how='left')
    #
    # traincode3.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode1.csv', index=False)
    # testcode3.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode1.csv', index=False)

if __name__ == '__main__':
    # train()
    test()
    # code10()