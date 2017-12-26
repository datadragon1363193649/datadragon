# -*- cocoding: utf-8 -*-
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
def t1():
    s1_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_11'
    s2_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_12'
    s3_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_13'
    s4_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_14'
    s5_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_15'
    slist=[]
    slist.append(s1_file)
    slist.append(s2_file)
    slist.append(s3_file)
    slist.append(s4_file)
    slist.append(s5_file)
    e1_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_11.xlsx'
    e2_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_12.xlsx'
    e3_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_13.xlsx'
    e4_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_14.xlsx'
    e5_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_15.xlsx'
    elist=[]
    elist.append(e1_file)
    elist.append(e2_file)
    elist.append(e3_file)
    elist.append(e4_file)
    elist.append(e5_file)

    for i in range(0,5):
        datalist=[]
        with open(slist[i],'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                datalist.append(linelist)
        fp.close()
        datadf=pd.DataFrame(datalist,columns=['phone','date','wecash','self'])
        datadf.to_excel(elist[i],index=False)
def stata_boxcox():
    s1_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/wecandselfscore_12'
    datalist = []
    with open(s1_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            datalist.append(float(linelist[-1]))
    boxdatam,_ =stats.boxcox(datalist)
    plt.figure(1)
    n, bins, patches = plt.hist(datalist, 100, normed=1, facecolor='g', alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()
    plt.figure(2)
    n, bins, patches = plt.hist(boxdatam, 100, normed=1, facecolor='g', alpha=0.75)
    plt.xlabel('hist')
    plt.ylabel('Probability')
    plt.title('Histogram')
    # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    plt.grid(True)
    plt.show()
if __name__ == '__main__':
    stata_boxcox()


