# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')
def marge1():
    sfile1='/Users/ufenqi/Documents/dataming/base1/test/uid_phn_train.txt1'
    sfile2='/Users/ufenqi/Documents/dataming/base1/test/uid_phn_train.txt'
    sfile3='/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/tcalldata.csv'
    outfile='/Users/ufenqi/Documents/dataming/base1/test/calldata'
    wfp=open(outfile,'w')
    uid_phn_dic={}
    with open(sfile2,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            uid_phn_dic[linelist[0]]=linelist[1]
    sfdic1={}
    with open(sfile1,'r') as fp:
        for line in fp:
            linelist=line.strip().split('\t')
            sfdic1[linelist[0]]=linelist[1:]
    with open(sfile3,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            if linelist[0] in uid_phn_dic:
                wfp.write(uid_phn_dic[linelist[0]]+'\t'+'\t'.join(linelist[1:])+'\t'+'\t'.join(sfdic1[uid_phn_dic[linelist[0]]])+'\n')
def marge2():
    sfile3 = '/Users/ufenqi/Documents/dataming/base1/data/data_1510/bank/tcalldata.csv'
    outfile = '/Users/ufenqi/Documents/dataming/base1/test/calldata_target'
    wfp = open(outfile, 'w')
    with open(sfile3,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            wfp.write(linelist[0]+','+linelist[1]+'\n')
def marge3():
    sfile3 = '/Users/ufenqi/Documents/dataming/base1/test/calldata_610_all'
    sfile2='/Users/ufenqi/Downloads/query_result.csv'
    outfile = '/Users/ufenqi/Documents/dataming/base1/test/calldata_610_all_update_target_fu'
    outfile1 = '/Users/ufenqi/Documents/dataming/base1/test/calldata_610_all_update_target_zheng'
    uid_phn_dic = {}
    zheng=0
    fu=0
    with open(sfile2, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            if float(linelist[1])<30:
                uid_phn_dic[linelist[0]] = '1'
                zheng+=1
            else:
                uid_phn_dic[linelist[0]] = '0'
                fu+=1
    print zheng,fu
    wfp = open(outfile, 'w')
    wfp1 = open(outfile1, 'w')
    with open(sfile3, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            if linelist[0] in uid_phn_dic:
                if uid_phn_dic[linelist[0]]=='1':
                    wfp1.write(linelist[0] + '\t' + uid_phn_dic[linelist[0]] + '\t' + '\t'.join(linelist[2:]) + '\n')
                else:
                    wfp.write(linelist[0] + '\t' + uid_phn_dic[linelist[0]] + '\t' + '\t'.join(linelist[2:]) + '\n')
def marge4():
    sfile1 = '/Users/ufenqi/Documents/dataming/base1/test/score_8201_1'
    sfile2 = '/Users/ufenqi/Documents/dataming/base1/test/score_8201_2'
    outfile = '/Users/ufenqi/Documents/dataming/base1/test/score_8201'
    wfp = open(outfile, 'w')
    uid_phn_dic = {}
    with open(sfile2, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            uid_phn_dic[linelist[0]] = linelist[1:]
    with open(sfile1, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            if linelist[0] in uid_phn_dic:
                wfp.write('\t'.join(linelist) + '\t' + '\t'.join(uid_phn_dic[linelist[0]]) + '\n')
def marge5():
    sfile1 = '/Users/ufenqi/Documents/dataming/base1/test/calldata_610_all_update_target_end'
    sfile2 = '/Users/ufenqi/Documents/dataming/base1/test/sample_phn_fu_yu_split1'
    outfile = '/Users/ufenqi/Documents/dataming/base1/test/calldata_610_all_add'
    wfp = open(outfile, 'w')
    uid_phn_dic = {}
    with open(sfile1, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            uid_phn_dic[linelist[0]] = linelist[1:]
    with open(sfile1, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            if linelist[0] in uid_phn_dic:
                wfp.write('\t'.join(linelist) + '\t' + '\t'.join(uid_phn_dic[linelist[0]]) + '\n')


if __name__ == '__main__':
    marge4()