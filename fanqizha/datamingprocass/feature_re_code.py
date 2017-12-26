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



fenxiangdict['3_1']=[[13],[11,10],[8,12,5],[9,2],[3,7,4],[1,6],[0]]
fenxiangdict['4_1']=[[1],[2],[3],[6,4,5],[7],[8,0,13],[12,11,9,10]]
fenxiangdict['5_1']=[[4],[5,11],[8,12],[10,2,7],[1,0,6],[13,3,9],[14]]
fenxiangdict['6_1']=[[3],[1],[4,2],[5,9,6,0],[7,8,10],[11],[14,12,13]]
fenxiangdict['7_1']=[[4,3],[1,5,2],[14],[7,0],[6,9,8],[11,12],[10,13]]
fenxiangdict['8_1']=[[14],[13,12,0],[1],[2,10,11,3],[9,4],[5,8,6],[7]]
fenxiangdict['9_1']=[[14],[0],[1],[4,2,13,3,12],[11,5],[9,10,6],[7,8]]
fenxiangdict['10_1']=[[0],[1,2],[14],[3,4],[6,5],[11,],[8,7,10,12,13,9]]
fenxiangdict['11_1']=[[14],[11,1,13],[0],[12],[3,10],[8,6,5,2,9,4],[7]]
fenxiangdict['12_1']=[[0],[1],[4,2],[5,3],[6,13,14,11],[7,8,9],[12,10]]
fenxiangdict['13_1']=[[13,9],[11],[12,7],[2,5],[10,8,0],[4,3,1],[6]]
fenxiangdict['14_1']=[[14],[13],[0,12],[1],[11,10,2],[7,9,3,5],[8,6,4]]
fenxiangdict['15_1']=[[14],[13],[12,0],[1],[2,10,11,3,9,4],[5,8,6],[7]]
fenxiangdict['16_1']=[[14,1],[0,13],[11,3],[8],[2,12,10,9],[4,6,7],[5]]
fenxiangdict['17_1']=[[14],[13],[12,11,6,10],[8],[9,0,7],[2,5,4,1],[3]]
fenxiangdict['18_1']=[[14],[12,11],[9,5],[10,3,7,13],[8,2,6],[0,4],[1]]
fenxiangdict['21_1']=[[0],[1,3],[5,4,2,9],[6,7],[8],[11,12,10],[14,13]]
fenxiangdict['22_1']=[[0],[1],[3,6],[4],[2],[5,8,9,11,12,10],[7]]
fenxiangdict['23_1']=[[7],[1,3,0],[2,4],[5],[6,8],[10],[9]]
fenxiangdict['25_1']=[[0],[1],[2],[4,6,3],[5],[8,7],[9]]
fenxiangdict['26_1']=[[0],[7],[2],[6,3],[4],[5],[1]]
fenxiangdict['27_1']=[[14],[13,0],[12,10,11],[9,5,3,1],[8],[4],[6,2,7]]
fenxiangdict['28_1']=[[14],[13],[0],[5,1,10,11],[9,2,12,4],[3,7,8],[6]]
fenxiangdict['29_1']=[[13],[12],[11,3],[2,1,10],[8,6,5,9],[7,4],[0]]
fenxiangdict['30_1']=[[0],[14],[8,12],[7],[13,4,9,3],[11,1,2,10],[5,6]]
fenxiangdict['31_1']=[[0],[14],[7,11,1,13],[9,2,4,12],[3,8],[6],[10,5]]
fenxiangdict['32_1']=[[13],[12],[11],[5,9,1],[8,7],[4,3,10],[2,0,6]]
fenxiangdict['33_1']=[[0],[3],[1,4,6,5],[2,7,14],[12,10],[8],[9,13,11]]
fenxiangdict['34_1']=[[14],[13,12],[0],[5],[1],[9,10,2,3,11],[8,7],[4,6]]
fenxiangdict['35_1']=[[13],[12],[9,11,7],[10],[8,2,1,5],[4,6,3],[0]]
fenxiangdict['39_1']=[[0],[8],[1,9,2,11],[13,3,7,10],[4,12],[14,6],[5]]
fenxiangdict['40_1']=[[0],[1,3],[2,4],[5],[6,7,9,11,8,10],[12,13],[14]]
fenxiangdict['41_1']=[[0],[9],[10,12],[2,1,11,13],[3,4,14],[5,8,6],[7]]
fenxiangdict['42_1']=[[0],[3,1],[4,2],[5,7,6,10],[12,8,11],[9,13],[14]]
fenxiangdict['43_1']=[[0],[9],[8],[10],[6,1],[3,2,5,4],[7]]
fenxiangdict['44_1']=[[0],[6,11,5],[10,8],[7,4,12],[9,3],[2],[1]]
fenxiangdict['45_1']=[[0],[6,11],[4,10,5,9],[2,7,8,12,3],[1]]
fenxiangdict['52_1']=[[0],[1,2],[4,3,7],[5],[9,8],[6],[10]]
fenxiangdict['53_1']=[[0],[4],[3],[1,2,6],[7],[5],[8,9]]
fenxiangdict['54_1']=[[0],[2,1],[3],[4],[5,7],[10,9],[6,8]]
fenxiangdict['55_1']=[[0],[3],[2,1],[6],[7,4,5]]
# fenxiangdict['56_1']=[[0],[1],[2,3,4],[5],[6],[7,10,8],[9]]





def traincode():
    f_file = '/Users/ufenqi/Downloads/fanqizha/traindfenxiang1.csv'
    fdf = pd.read_csv(f_file)
    # print fdf.head()
    dfcol=fdf.columns
    realcol=[]
    realcol.append('0')
    realcol.append('1')
    realcol.append('2')
    realcol.append('19_1')
    realcol.append('24_1')
    realcol.append('45_1')
    realcol.append('50_1')
    realcol.append('51_1')
    realcol.append('55_1')
    realcol.append('62_1')
    realcol.append('63_1')


    for col in dfcol:
        if col in fenxiangdict.keys():
            if col!='45_1' and col!='55_1':
                realcol.append(col)
            dl=[]
            dr=[]
            for i in range(0,len(fenxiangdict[col])):
                for v in fenxiangdict[col][i]:
                    dl.append(v)
                    dr.append(i)
            print dl,dr
            fdf[col].replace(dl,dr,inplace=True)
    # del fdf['56_1']
    feanewdf=fdf[realcol]
    feanewdf.to_csv('/Users/ufenqi/Downloads/fanqizha/traindfenxiang_code.csv',index=False)
def testcode():
    f_file = '/Users/ufenqi/Downloads/fanqizha/testfenxiang'
    traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/traindfenxiang_code.csv')
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
    testdf.to_csv('/Users/ufenqi/Downloads/fanqizha/testfenxiang_code.csv', index=False)
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
    testcode()



