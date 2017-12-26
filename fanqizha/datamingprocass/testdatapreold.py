# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
# 原始数据
f_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdatamax1'
col_file = '/Users/ufenqi/Downloads/traincol'
thb_file = '/Users/ufenqi/Downloads/tianhuaban'
fenxiang_file='/Users/ufenqi/Downloads/fenxiang1'
# train_file = '/Users/ufenqi/Downloads/traindfdel1.csv'
out_file='/Users/ufenqi/Downloads/testfenxiangmax'
wfp = open(out_file, 'w')
fenxiangdict={}
with open(fenxiang_file, 'r') as fp:
    for line in fp:
        fenxianglist=line.strip().split('\t')
        fenintl=[]
        for i in fenxianglist[1:]:
            fenintl.append(float(i))
        fenxiangdict.setdefault(int(fenxianglist[0].split('_')[0]),fenintl)
columnlist=fenxiangdict.keys()
# columnlist.append(2)
# columnlist.append(65)
dflist=[]
with open(f_file, 'r') as fp:
    for line in fp:
        linelist = line.strip().split('\t')
        lin1 = [linelist[-2], linelist[-1]]
        for i in linelist[2:-2]:
            lin1.append(i)
        linelist=lin1
        dfl=[]
        # dfl.append(linelist[0])
        # dfl.append(linelist[1])
        # dfl.append(linelist[2])
        for i in range(0,len(linelist)):
            if i in columnlist:
                fenintl=fenxiangdict.get(i)
                if float(linelist[i]) > fenintl[-1]:
                    dfl.append(str(len(fenintl) - 2))
                    print len(fenintl) - 2
                    continue
                for j in range(1, len(fenintl)):
                    if float(linelist[i]) <= fenintl[j]:
                        dfl.append(str(j - 1))
                        print str(j - 1)
                        break
            else:
                dfl.append(linelist[i])
        wfp.write('\t'.join(dfl)+'\n')
print columnlist
# print columnlist
# testdf=fdf[columnlist]
# for col in columnlist:
#     testdf[col]=testdf[col].astype('float64')
# # print testdf.describe()
# # with open(thb_file, 'r') as fp:
# #     for line in fp:
# #         thblist = line.strip().split('\t')
# #         col=int(thblist[0])
# for col in columnlist:
#     vvl = []
#     vll = list(set(testdf[col].values.tolist()))
#     fenintl = fenxiangdict.get(col)
#     for vl in vll:
#         if vl >fenintl[-1]:
#             vvl.append([vl, len(fenintl)-2])
#             continue
#         for i in range(1,len(fenintl)):
#             if vl<=fenintl[i]:
#                 vvl.append([vl, i-1])
#                 break
#     if len(vvl) != 0:
#         for vl in vvl:
#             testdf[col].replace(vl[0], vl[1], inplace=True)
#
# print testdf.head()
# testdf.to_csv('/Users/ufenqi/Downloads/test_fenxiang.csv',index=False)