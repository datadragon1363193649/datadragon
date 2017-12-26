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



fenxiangdict['3_1']=[[13],[12],[11],[10],[9,7,8,5,2,4,3],[6,1],[0]]
fenxiangdict['4_1']=[[1,2],[3,4],[0,14],[12,7],[11,5,9],[6,13,8],[10]]
fenxiangdict['5_1']=[[0],[8,6,10],[12,3,13],[7,9],[11,2,14,],[1,4],[5]]
fenxiangdict['6_1']=[[1],[3],[4,5,2],[6,8,7],[9,0,10,14],[11],[12,13]]
fenxiangdict['7_1']=[[5,4],[1],[6,2],[14,3,7,0],[8],[13,11,9,12],[10]]
fenxiangdict['8_1']=[[14],[0,13],[12],[1],[2,11,9,10,3],[4,8,5],[7,6]]
fenxiangdict['9_1']=[[14],[0,13],[11],[12,1,4,2,10,5,9],[3],[7,6],[8]]
fenxiangdict['10_1']=[[0],[1],[2],[4,14,7],[13,8,6],[3,5,10,9],[11,12]]
fenxiangdict['11_1']=[[14,11],[13],[0],[12,10,9],[5,1],[2,3,6],[4,7,8]]
fenxiangdict['12_1']=[[2,0],[1],[3],[5,6],[14,13],[4,8,10,12,9],[7,11]]
fenxiangdict['13_1']=[[14],[13],[11,12,3],[6],[4,0,9],[1,2,5],[8,7,10]]
fenxiangdict['14_1']=[[14],[13],[0],[1],[12,11,9,10,3],[8,2,7,4,6],[5]]
fenxiangdict['15_1']=[[14],[0,13],[12],[1],[2,11,9,10,3,4,8],[5],[7,6]]
fenxiangdict['16_1']=[[0],[14],[9],[1,7,6,10,2,12],[11,3,13],[4,5],[8]]
fenxiangdict['17_1']=[[14],[13],[11,12],[9,8,10,7],[6,0,3,2],[1],[5,4]]
fenxiangdict['18_1']=[[14],[12,11],[9,5],[10,3,7,13],[8,2,6],[0,4],[1]]
fenxiangdict['21_1']=[[0],[6,1,3,7],[2,4,11],[10,5],[9,13],[8],[12]]
fenxiangdict['22_1']=[[0,1],[8],[4,2],[3,12],[10],[5,9,11],[6,7]]
fenxiangdict['23_1']=[[7],[0],[3,11,8],[4,2],[10],[5,6,1],[9]]
fenxiangdict['24_1']=[[0],[6],[5],[4],[3],[1],[2]]
fenxiangdict['25_1']=[[0],[1],[3,5,2],[4],[6,8,7],[9],[10]]
fenxiangdict['26_1']=[[0],[6],[7],[2,4],[3],[5],[1]]
fenxiangdict['27_1']=[[13],[12],[11],[9,10,8],[0],[7,4,1,6],[5,2,3]]
fenxiangdict['28_1']=[[14],[13],[12],[0,11],[5,6,1,4],[8,9,10],[3,2,7]]
fenxiangdict['29_1']=[[14],[13],[12],[11],[0,9,10],[6,8,3,1,2,5],[4,7]]
fenxiangdict['30_1']=[[0],[14],[12,13,11,10,8],[6],[1,9,7],[4],[2,3,5]]
fenxiangdict['31_1']=[[0],[14],[12],[13,10],[11,8,9],[6,1,7,4,5,3],[2]]
fenxiangdict['32_1']=[[14,0],[13],[12],[11],[7,9,10,8,6],[2,1],[4,3,5]]
fenxiangdict['33_1']=[[0],[3,1],[5],[2,11],[4,14,9],[6,8,13,12,7],[10]]
fenxiangdict['34_1']=[[14],[13],[11,12],[0],[10,9,3],[4,6,1,5,2],[7,8]]
fenxiangdict['35_1']=[[14],[13],[12],[10],[11,7,8,2,9],[3,5,6,0],[4,1]]
fenxiangdict['39_1']=[[0],[1],[7,8],[9,2],[3,10],[11,13],[14,4,12,5,6]]
fenxiangdict['40_1']=[[0],[2,1],[4],[3,5],[7,6,9,11],[8,10],[14,12,13]]
fenxiangdict['41_1']=[[0],[9],[1,10],[2,12],[11,3,13,4],[14,8],[5,6,7]]
fenxiangdict['42_1']=[[0],[1,2],[4,3,5],[6,7,9],[11,10,8],[13],[12,14]]
fenxiangdict['43_1']=[[0],[6],[4,3],[5,7,1],[2]]
fenxiangdict['44_1']=[[0],[6,8],[3,5],[9,7],[4],[1],[2]]
fenxiangdict['45_1']=[[0],[9],[3,7,5,6,4],[8,2],[1]]
fenxiangdict['52_1']=[[0],[3],[1,4,2],[8],[6,5],[9],[7]]
fenxiangdict['53_1']=[[0],[2],[5,4],[3],[1],[7,6],[8]]
fenxiangdict['54_1']=[[0],[2,1,4],[8,3,6],[7,5]]
fenxiangdict['55_1']=[[0],[5],[4,3,2],[1],[6,7]]
# fenxiangdict['56_1']=[[0],[1],[2,3,4],[5],[6],[7,10,8],[9]]





def traincode():
    f_file = '/Users/ufenqi/Downloads/traindffenxiangold.csv'
    fdf = pd.read_csv(f_file)
    # print fdf.head()
    dfcol=fdf.columns
    for col in dfcol:
        if col in fenxiangdict.keys():
            dl=[]
            dr=[]
            for i in range(0,len(fenxiangdict[col])):
                for v in fenxiangdict[col][i]:
                    dl.append(v)
                    dr.append(i)
            print dl,dr
            fdf[col].replace(dl,dr,inplace=True)
    del fdf['56_1']
    fdf.to_csv('/Users/ufenqi/Downloads/traindfenxiang_codeold.csv',index=False)
def testcode():
    f_file = '/Users/ufenqi/Downloads/testfenxiangmax'
    ff = []
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    colist=[]
    colist.append(0)
    colist.append(1)
    colist.append(2)
    colist.append(63)
    for k in fenxiangdict.keys():
        colist.append(int(k.split('_')[0]))
    colist.sort()
    # print colist
    testdf=fdf[colist]
    for col in colist:
        testdf[col] = testdf[col].astype('int')
    # print fdf.head()
    for col in fenxiangdict:
        fxcol=int(col.split('_')[0])
        dl = []
        dr = []
        for i in range(0, len(fenxiangdict[col])):
            for v in fenxiangdict[col][i]:
                dl.append(v)
                dr.append(i)
        # print col,dl, dr
        dl.sort()
        print col, dl
        testdf[fxcol].replace(dl, dr, inplace=True)
    testdf.to_csv('/Users/ufenqi/Downloads/testfenxiang_codemax.csv', index=False)
if __name__ == '__main__':
    traincode()
