# -*- cocoding: utf-8 -*-
import pandas as pd
import sys
reload(sys)
import json

sys.setdefaultencoding('utf-8')
# 催收统计分析
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
# 筛选某些天数据
def filtrate_data():
    sfile = '/Users/ufenqi/Downloads/traindata_501_901'
    outfile='/Users/ufenqi/Downloads/data_711_f'
    wfp=open(outfile,'w')
    # datelist=['2017-7-1 ','2017-7-2 ','2017-7-3 ','2017-7-4 ','2017-7-5 ',
    #           '2017-7-6 ','2017-7-7 ','2017-7-8 ','2017-7-9 ','2017-7-10',
    #           '2017-7-11','2017-7-12','2017-8-21','2017-8-22','2017-8-23',
    #           '2017-8-24','2017-8-25','2017-8-26','2017-8-27','2017-8-28',
    #           '2017-8-29','2017-8-30','2017-8-31']
    datelist=['2017-7-1 ','2017-7-2 ','2017-7-3 ','2017-7-4 ','2017-7-5 ',
              '2017-7-6 ','2017-7-7 ','2017-7-8 ','2017-7-9 ','2017-7-10',
              '2017-7-11','2017-7-12']
    num=0
    numfu=0
    with open(sfile, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            # if '2017-9' in linelist[1]:
            #     if float(linelist[-1])>-1:
            #         num+=1
            #     if float(linelist[-1])>29:
            #         # print line
            #         numfu+=1
                # wfp.write(line.strip()+'\n')
            datestr=linelist[1][:9]
            if '2017-5' in linelist[1]:
                continue
            if '2017-6' in linelist[1]:
                continue
            if '2017-9' in linelist[1]:
                continue
            if datestr in datelist:
                continue
            if float(linelist[-1]) > -1:
                wfp.write(line.strip()+'\n')
    # print num
    # print numfu
    # print numfu*1.0/num

if __name__ == '__main__':
    filtrate_data()