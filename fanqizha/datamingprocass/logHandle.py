# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
# pd.set_option('precision', 10)
def train():
    def loghandle():
        # traindatadf = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfdelinner.csv')
        traindatadf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/traindfdelinner3.csv')

        # testdatadf = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturedel.csv')
        out_file = '/Users/ufenqi/Documents/dataming/base1/config/loginvalue.json'
        # s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_my'
        # feaidlist = []
        # feanamelist = []
        # feaidlist.append('0')
        # with open(s_file, 'r') as fp:
        #     for line in fp:
        #         linelist = line.strip().split(',')
        #         feaidlist.append(linelist[0])
        #         feanamelist.append(linelist[1])
        # columns = feaidlist
        wfp = open(out_file, 'w')
        import seaborn as sns
        feacol = traindatadf.columns.tolist()
        feacol.remove('0')
        feacol.remove('1')
        feacol.remove('2')
        feacol.remove('63')
        # feacol = feaidlist
        # feacol.remove('2')
        # feacol.remove('63')
        recol = []
        recol.append('0')
        recol.append('1')
        recol.append('2')
        recol.append('63')
        logdic = {}
        for col in feacol:
            # print len(feacol)
            a = traindatadf[col] - traindatadf[col].min()
            traindatadf[col + 'log'] = np.log(a + 1)
            if len(traindatadf[col + 'log'].unique()) < 2:
                continue
            # wfp.write(col+'\t'+str(traindatadf[col].min())+'\n')
            logdic[col] = round(traindatadf[col].min(), 10)
            recol.append(col + 'log')
            # t = testdatadf[col] - traindatadf[col].min()
            # testdatadf[col + 'log'] = np.log(t + 1)
        json.dump(logdic, wfp)
        trainredf = traindatadf[recol]

        trainredf = trainredf.round(10)
        # testredf = testdatadf[recol]
        # testredf = testredf.round(10)
        wfp.close()
        trainredf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/traindfloginner3.csv',index=False)
        # testredf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturelog.csv', index=False)
        return recol

    # 归一化
    def normalizing(re):
        traindatadf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/traindfloginner3.csv')
        # testdatadf = pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturelog.csv')
        out_file = '/Users/ufenqi/Documents/dataming/base1/config/norinvalue.json'
        fea_file = '/Users/ufenqi/Documents/dataming/base1/config/featurecallnor'
        ffp = open(fea_file, 'w')
        wfp = open(out_file, 'w')
        import seaborn as sns
        feacol = traindatadf.columns.tolist()
        # feacol = re
        feacol.remove('0')
        feacol.remove('1')
        feacol.remove('2')
        feacol.remove('63')

        # feacol.remove('0')
        # feacol.remove('1')
        # feacol.remove('2')
        # feacol.remove('63')
        # feacol = re
        recol = []
        recol.append('0')
        recol.append('1')
        recol.append('2')
        recol.append('63')
        nordic = {}
        for col in feacol:
            a1 = traindatadf[col] - traindatadf[col].min()
            a2 = traindatadf[col].max() - traindatadf[col].min()
            traindatadf[col + 'nor'] = a1 / a2
            recol.append(col + 'nor')
            # t1 = testdatadf[col] - traindatadf[col].min()
            # testdatadf[col + 'nor'] = t1 / a2
            nordic[col] = [round(traindatadf[col].min(), 10), round(traindatadf[col].max(), 10)]
            # wfp.write(col + '\t' + str(traindatadf[col].max())+'\t' + str(traindatadf[col].min()) + '\n')
        json.dump(nordic, wfp)
        ffp.write(','.join(recol) + '\n')
        trainredf = traindatadf[recol]
        trainredf = trainredf.round(10)
        # testredf = testdatadf[recol]
        # testredf = testredf.round(10)
        trainredf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/traindfnorinner3.csv',index=False)
        # testredf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/userfeaturenor.csv', index=False)

    re = loghandle()
    normalizing(re)
def test():
    def loghandle():
        traindatadf=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/traindfdelinner3.csv')
        testdatadf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeaturedel_501.csv')
        out_file='/Users/ufenqi/Documents/dataming/base1/config/loginvalue.json'
        s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
        feaidlist = []
        feanamelist = {}
        # feaidlist.append('0')
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                feanamelist[linelist[0]]=linelist[1]
        columns = feaidlist
        wfp=open(out_file,'w')
        import seaborn as sns
        feacol=traindatadf.columns.tolist()
        feacol.remove('0')
        feacol.remove('1')
        feacol.remove('2')
        feacol.remove('63')
        feacol=feaidlist
        feacol.remove('2')
        feacol.remove('63')
        recol=[]
        recol.append('0')
        # recol.append('1')
        recol.append('2')
        recol.append('63')
        recoldic={}
        logdic={}
        for col in feacol:
            # print len(feacol)
            a=traindatadf[col]-traindatadf[col].min()
            traindatadf[col+'log']=np.log(a+1)
            if len(traindatadf[col+'log'].unique()) < 2:
                continue
            # wfp.write(col+'\t'+str(traindatadf[col].min())+'\n')
            logdic[feanamelist[col]]=round(traindatadf[col].min(),10)
            recol.append(col+'log')
            recoldic[col+'log']=feanamelist[col]
            t=testdatadf[col]-traindatadf[col].min()
            testdatadf[col + 'log'] = np.log(t + 1)
        json.dump(logdic,wfp)
        trainredf=traindatadf[recol]

        trainredf=trainredf.round(10)
        testredf = testdatadf[recol]
        testredf = testredf.round(10)
        wfp.close()
        # trainredf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfloginner.csv',index=False)
        testredf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeaturelog_501.csv', index=False)
        return recol,recoldic
    #归一化
    def normalizing(re,redic):
        traindatadf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/traindfloginner3.csv')
        testdatadf = pd.read_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeaturelog_501.csv')
        out_file = '/Users/ufenqi/Documents/dataming/base1/config/norinvalue.json'
        fea_file='/Users/ufenqi/Documents/dataming/base1/config/featurecallnor1'
        ffp=open(fea_file,'w')
        wfp = open(out_file, 'w')
        import seaborn as sns
        feacol = traindatadf.columns.tolist()
        feacol = re
        feacol.remove('0')
        # feacol.remove('1')
        feacol.remove('2')
        feacol.remove('63')


        # feacol.remove('0')
        # feacol.remove('1')
        # feacol.remove('2')
        # feacol.remove('63')
        # feacol=re
        recol = []
        recollist=[]
        recol.append('0')
        recollist.append(['0','phn'])
        # recol.append('1')
        recol.append('2')
        recollist.append(['2','gender'])
        recol.append('63')
        recollist.append(['63','source_type'])
        nordic={}
        for col in feacol:
            a1 = traindatadf[col] - traindatadf[col].min()
            a2 = traindatadf[col].max()-traindatadf[col].min()
            traindatadf[col+'nor']=a1/a2
            recol.append(col + 'nor')
            recollist.append([col + 'nor',redic[col]])
            t1=testdatadf[col] - traindatadf[col].min()
            testdatadf[col + 'nor'] = t1 / a2
            nordic[redic[col]]=[round(traindatadf[col].min(),10),round(traindatadf[col].max(),10)]
            # wfp.write(col + '\t' + str(traindatadf[col].max())+'\t' + str(traindatadf[col].min()) + '\n')
        json.dump(nordic,wfp)
        for rec in recollist:
            ffp.write(rec[0]+','+rec[1]+'\n')
        trainredf = traindatadf[recol]
        trainredf = trainredf.round(10)
        testredf = testdatadf[recol]
        testredf = testredf.round(10)
        # trainredf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnorinner.csv',index=False)
        testredf.to_csv('/Users/ufenqi/Documents/dataming/base1/data/userfeaturenor_501.csv', index=False)

    re,redic = loghandle()
    normalizing(re,redic)
if __name__ == '__main__':
    # train()
    test()
# sns.distplot(traindatadf['5'])
# traindatadf['7log']=np.log(traindatadf['7']+1)
# fu=np.log(abs(traindatadf['8'][traindatadf['8']<0])+1)*-1
# zheng=np.log(traindatadf['8'][traindatadf['8']>=0]+1)
# traindatadf['8log']=fu.append(zheng)

