#! /usr/bin/env python
# -*- encoding: utf-8 
import os
import sys
import datetime

def f():
    for line in sys.stdin:
        line = line.strip().replace('"', '')
        uid, ud = line.split(',')
        d = datetime.datetime.strptime(ud, "%Y/%m/%d %H:%M:%S")
        h = d.hour
        w = d.weekday()
        f1 = 0 # 时间段
        f2 = 2 # 是否周日
        if (h >= 8 and h < 11):
            f1 = 0
        elif (h >= 11 and h < 14):
            f1 = 1
        elif (h >= 14 and h < 18):
            f1 = 2
        elif (h >= 18 and h < 21):
            f1 = 3
        else:
            f1 = 4
        if w < 5:
            f2 = 0
        else:
            f2 = 1
            
        print uid + '\t' + str(f1) + '\t' + str(f2)

def m():
    f1 = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/train_fea_new'
    f2 = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/train_fea_old'
    with open(f1, 'r') as rfp1, open(f2, 'r') as rfp2:
        nf = {}
        for line in rfp1:
            linelist = line.strip().split('\t')
            nf[linelist[0]] = linelist[2:]
        for line in rfp2:
            linelist = line.strip().split('\t')
            if linelist[0] in nf:
                print '\t'.join(linelist + nf[linelist[0]])



def e():
    f1 = '/home/liuxiaoliang/workspace/feature/working/raw_data/train_all'
    f2 = '/home/liuxiaoliang/workspace/feature/working/call_data/url/mobile_detail.txt'
    with open(f1, 'r') as rfp1, open(f2, 'r') as rfp2:
        ulist = set()
        for line in rfp1:
            ulist.add(line.split('\t')[0])
        for line in rfp2:
            if line.strip().split('\t')[0] in ulist:
                print line.strip()
def d():
    for line in sys.stdin:
        linelist = line.strip().split('\t')
        linelist = linelist[:3] + linelist[5:]
        print '\t'.join(linelist)

def a():
    lablef = '/home/liuxiaoliang/workspace/feature/working/exp/uid_odd'
    fea = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1/uid_fea'
    lable = {}
    with open(lablef, 'r') as fp:
        for line in fp:
            uid, l = line.strip().split('\t')
            lable[uid] = l
    with open(fea, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            uid = linelist[0]
            if uid in lable:
                n = linelist[:]
                n.insert(1, lable[uid])
                print '\t'.join(n)

def t():
    pro_file = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/online/over_day.csv'
    pool = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/online/auth_num_all_20161115_20161220.csv'
    u = []
    with open(pro_file, 'r') as rfp:
        for line in rfp:
            u.append(line.strip().split(',')[0])
    with open(pool, 'r') as rfp:
        for line in rfp:
            if line.strip().replace('"', '').split(',')[0] in u:
                print line.strip()

def t1():
    f1 = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/online/s_1'
    f2 = '/home/liuxiaoliang/workspace/feature/working/exp/fea_1_2/online/uid_odd'
    u = []
    with open(f1, 'r') as fp:
        for line in fp:
            u.append(line.split('\t')[0])
    with open(f2, 'r') as fp:
        for line in fp:
            if line.split('\t')[0] in u:
                print line.strip()

if __name__ == '__main__':
    t1()
