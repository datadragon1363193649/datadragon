# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
fenxiangdict={}
# fenxiangdict['3_1']=[[11],[14],[4,7],[6],[3,5,9,10],[13,12,8]]
# fenxiangdict['11_1']=[[8,6],[10],[3,5,13],[12,1,4],[11,2,7,14,9,0]]
# fenxiangdict['12_1']=[[2,5],[7],[14,9,6,13],[11,4,3,0],[10,8,1],[12]]
# fenxiangdict['17_1']=[[2,9,10,7,13,0,5,3,4,8],[12,6,14,1],[11]]
# fenxiangdict['18_1']=[[0,9,5],[13,6,3,1,7],[10,12,8,2,4,14,11]]
# fenxiangdict['21_1']=[[2],[10,7,4],[0,11,9,1],[3,5],[8,6]]
# fenxiangdict['25_1']=[[5],[3],[1],[6,2],[4],[0]]
# fenxiangdict['27_1']=[[2],[0,1],[9,13,10,7],[8,5,4,6,12,3],[11]]
# fenxiangdict['28_1']=[[9],[12],[3,13,8],[1,15,11,2,5],[4,10,6,0,7,14]]
# fenxiangdict['30_1']=[[3,6],[5,0],[1,14,12,4,10,2],[15,11,7],[13,8,9]]
# fenxiangdict['31_1']=[[2],[0],[12,10],[9,1,5,14],[7,13,11],[8,15,6],[3,4]]
# fenxiangdict['34_1']=[[9],[4],[10,12,6],[3,1,0],[2,11,15,8,7,13,14,5]]
# fenxiangdict['42_1']=[[14],[15,3,7,9,12],[8,4,5,13],[11,10,0,2],[1,6]]



fenxiangdict['3']=[[14],[13],[11],[12,8,5,10],[9,3,2,7,4],[6,1],[0]]
fenxiangdict['4']=[[1,2],[3],[4,7,13,5,6],[10,0],[8,11],[12],[9]]
fenxiangdict['5']=[[4],[5,11],[10,3,7],[9,1,2,6],[8,0,12],[13],[14]]
fenxiangdict['6']=[[3,1,4,2],[8],[0,6,5],[9],[7,10],[12,11,13],[14]]
fenxiangdict['7']=[[3,4],[1],[2,5,14],[0,7,6],[8,13,11],[9,10],[12]]
fenxiangdict['8']=[[14],[13],[12],[0,11,1],[3,9],[10,2,4],[8,7,5,6]]
fenxiangdict['9']=[[14],[13,0],[2],[1,4],[12,6,9],[10,11,3],[5,8,7]]
fenxiangdict['10']=[[0],[14],[1,2],[5,4,3],[8,13,12],[6,11,9],[10,7]]
fenxiangdict['11']=[[14],[13],[1,0,10,11],[12],[3],[6,9,5],[2,8,4,7]]
fenxiangdict['12']=[[0,1],[4,14],[2,3,6],[5,12],[8,10,13],[11,7],[9]]
fenxiangdict['13']=[[14],[7],[13],[9,11,5,10],[2],[0,12,3,1,4,8],[6]]
fenxiangdict['14']=[[14],[13],[0,12],[1,11],[10,2,9,6],[3,8,5,7],[4]]
fenxiangdict['15']=[[14],[13],[12],[0,11,1],[3,9],[10,2,4],[8,7,5,6]]
fenxiangdict['16']=[[14],[13,0],[2,1,11],[10,12,8],[3],[5,4,7],[9,6]]
fenxiangdict['17']=[[14],[13],[10,12],[11,6],[9,8],[5,7,0],[4,1,2,3]]
fenxiangdict['18']=[[13],[14,11],[12,9,7],[8,10,5,6],[2,3,4],[0],[1]]
fenxiangdict['19']=[[4],[3],[1],[0]]
fenxiangdict['21']=[[0],[2],[1,4,3],[5,7,6],[12,8,9],[10,11],[13]]
fenxiangdict['22']=[[0],[1],[2],[3,4],[7,6,5,9],[10,8,13,12],[11]]
fenxiangdict['23']=[[2,0],[7],[8,4,1],[3],[5],[10,6],[9,11]]
fenxiangdict['24']=[[0],[6],[5],[1],[3],[2],[4]]
fenxiangdict['25']=[[0],[1,2],[6,3],[5,4,7],[8],[9,10],[11]]
fenxiangdict['26']=[[0],[7],[1],[3],[8],[5,4,6],[2]]
fenxiangdict['27']=[[14],[13],[0],[12],[11,9,10],[5,1,8],[2,3,4,6,7]]
fenxiangdict['28']=[[14],[13],[12,0],[1,2,10,4,9],[11,7],[8,5],[3,6]]
fenxiangdict['29']=[[14],[13],[1,12],[3,4,10,2,6,11],[8,9],[5,7],[0]]
fenxiangdict['30']=[[0],[14],[12],[7,8,13],[9,3],[11,4,2,1],[5,6,10]]
fenxiangdict['31']=[[0],[14],[7],[12,13,1],[4,9,11,2,3],[10,6,8],[5]]
fenxiangdict['32']=[[14],[1],[13],[12,11],[2,8,9],[5,7,3,6,10,4],[0]]
fenxiangdict['33']=[[0,3],[5,1],[4,14,2],[6],[11,8,7],[9,13],[12,10]]
fenxiangdict['34']=[[14],[13],[12],[0,11],[3,8,1,6,10],[5,2,4],[9,7]]
fenxiangdict['35']=[[14],[13],[12],[11,9,8],[10,2,3,5,6,1],[7,4],[0]]
fenxiangdict['39']=[[0],[1,7],[8,2,9,10],[3,12],[11,13],[5,4,6],[14]]
fenxiangdict['40']=[[0,1,3],[2],[4,5],[7],[9,10,8],[6,11,12],[14,13]]
fenxiangdict['41']=[[0],[9],[11],[2,1,10,12,13],[3,4],[8,5,6],[7,14]]
fenxiangdict['42']=[[0],[1,3],[4,2],[5],[10,6,7],[11,9,8,12],[13,14]]
fenxiangdict['43']=[[0],[10],[9],[2],[5,8,1,3,6],[4,7],[11]]
fenxiangdict['44']=[[0],[11,5],[7,6,8],[12,13,10,9],[4,3],[2],[1]]
fenxiangdict['45']=[[0],[12,11],[5,6,9],[8,7],[3,4,10],[2],[1,13]]
fenxiangdict['50']=[[0],[1],[2],[3]]
fenxiangdict['51']=[[0],[2],[1],[3]]
fenxiangdict['52']=[[0],[1],[3,2],[5,4,6],[9,7,8,10],[11],[12]]
fenxiangdict['53']=[[0],[1],[4],[3,5,2],[7,6],[10,8],[9]]
fenxiangdict['54']=[[0],[1],[2,3],[4],[11,5,6],[6,10,9],[8,7]]
fenxiangdict['55']=[[0],[1],[4],[3,2],[9,8,6],[5],[7]]
fenxiangdict['62']=[[0],[4],[5],[6],[2],[1],[3]]
fenxiangdict['63']=[[0],[1],[4],[3],[2]]




def traincode():
    f_file = '/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainfenxiang.csv'
    fdf = pd.read_csv(f_file)
    # print fdf.head()
    dfcol=fdf.columns
    realcol=[]
    realcol.append('0')
    realcol.append('1')
    realcol.append('19')
    # realcol.append('24_1')
    # realcol.append('26_1')
    # realcol.append('38_1')
    realcol.append('50')
    realcol.append('51')
    # realcol.append('62_1')
    realcol.append('63')


    for col in dfcol:
        if col in fenxiangdict.keys():
            if col not in ['19','50','51','63']:
                realcol.append(col)
                realcol.append(col+'_1')
            dl=[]
            dr=[]
            for i in range(0,len(fenxiangdict[col])):
                for v in fenxiangdict[col][i]:
                    dl.append(v)
                    dr.append(i)
            print dl,dr
            fdf[col+'_1']=fdf[col]
            fdf[col].replace(dl,dr,inplace=True)
    # del fdf['56_1']
    feanewdf=fdf[realcol]
    feanewdf.to_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainfenxiang_code.csv',index=False)
def testcode():
    f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizha/testfenxiang'
    traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizha/traindfenxiang_code.csv')
    traincol=traindf.columns
    colist = []
    for col in traincol:
        colist.append(int(col.split('_')[0]))
    ff = []
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    testdf=fdf[colist]
    for col in colist:
        testdf[col] = testdf[col].astype('int')
    for col in colist:
        strcol=str(col)+'_1'
        if strcol in fenxiangdict.keys():
            print strcol
            dl=[]
            dr=[]
            for i in range(0,len(fenxiangdict[strcol])):
                for v in fenxiangdict[strcol][i]:
                    dl.append(v)
                    dr.append(i)
            print dl,dr
            testdf[col].replace(dl,dr,inplace=True)
            print col
            print testdf[col].value_counts()
    testdf.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizha/testfenxiang_code.csv', index=False)
    # print testdf.head()
    # colist.append(0)
    # colist.append(1)
    # colist.append(2)
    # colist.append(65)
    # for k in fenxiangdict.keys():
    #     colist.append(int(k.split('_')[0]))
    # colist.sort()
    # # print colist
    # testdf=fdf[colist]
    # for col in colist:
    #     testdf[col] = testdf[col].astype('int')
    # # print fdf.head()
    # for col in fenxiangdict:
    #     fxcol=int(col.split('_')[0])
    #     dl = []
    #     dr = []
    #     for i in range(0, len(fenxiangdict[col])):
    #         for v in fenxiangdict[col][i]:
    #             dl.append(v)
    #             dr.append(i)
    #     # print col,dl, dr
    #     dl.sort()
    #     print col, dl
        # testdf[fxcol].replace(dl, dr, inplace=True)
    # testdf.to_csv('/Users/ufenqi/Downloads/testfenxiang_code.csv', index=False)
if __name__ == '__main__':
    traincode()



