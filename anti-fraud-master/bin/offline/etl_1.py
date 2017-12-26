#! /usr/bin/env python
# -*- cocoding: utf-8 -*-

import sys
import os
import time
import datetime
import numpy as np
import math
import json
from scipy import stats
from sklearn.utils.multiclass import type_of_target
from sklearn.decomposition import PCA

class FeaEtl(object):
    "process feature"
    def __init__(self):
        pass
        
    def merge_fea(self):
        """
        合并 fea_1 fea_2
        """
        f1 = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_1'
        f2 = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_2'
        f3 = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea'
        with open(f1, 'r') as f1p, open(f2, 'r') as f2p, open(f3, 'w') as f3p:
            fea_1 = {}
            for line in f1p:
                linelist = line.strip().split('\t')
                uid, gender, age = linelist
                fea_1[uid] = linelist[1:]
            for line in f2p:
                linelist = line.strip().split('\t')
                uid = linelist[0]
                if uid in fea_1:
                    f3p.write(uid + '\t' + '\t'.join(fea_1[uid] + linelist[1:])+ '\n')
        
    def get_bin(self):
        """
        特征值个数<=5的特征不做分箱,但要统一从0分段
        """
        inf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/feature/fea_30'
        #outf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/feature/fea_30_bin'
        binf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/feature/bins'
        bins = {}
        uid_fea = {}
        uid_lbl = {}
        fea_len = 0
        smpl_len = 0
        with open(inf, 'r') as fp:
            for line in fp:
                smpl_len += 1
                linelist = line.strip().split('\t')
                uid = linelist[0]
                lbl = linelist[1]
                fea = linelist[2:]
                fea = [float(i) for i in fea]
                uid_fea[uid] = fea
                fea_len = len(fea)
                uid_lbl[uid] = lbl
        
        # 每个特征做分箱
        step = 1500
        for i in range(fea_len):
            ifea = []
            ifea_set = set()
            for u in uid_lbl:
                ifea.append((uid_lbl[u], float(uid_fea[u][i])))
                ifea_set.add(float(uid_fea[u][i]))
            ifea.sort(key = lambda x:x[1])
            ifea_list = list(ifea_set)
            ifea_list.sort()
            if len(ifea_list) <= 5:
                bins[i] = ifea_list
                continue
            bin_list = []
            for j in range(step, len(ifea), step):
                bin_list.append(ifea[j][1])
            bins[i] = bin_list
        #print bins
        new_bins = {}
        for k, v in bins.items():
            new_bins[k] = sorted(list(set(v)))
        #print new_bins
        with open(binf, 'w') as fp:
            fp.write(json.dumps(new_bins) + '\n')
            
    def fea_bin(self):
        inf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_lbl'
        binsf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/feature/bins'
        outf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_lbl_bin'
        bins = {}
        with open(binsf, 'r') as fp:
            bins = json.loads(fp.readline().strip())
        with open(inf, 'r') as fp, open(outf, 'w') as wfp:
            for line in fp:
                linelist = line.strip().split('\t')
                uid = linelist[0]
                lbl = linelist[1]
                fea = linelist[2:]
                fea = [float(i) for i in fea]
                for i in range(len(fea)):
                    nv = 0
                    cfv = fea[i]
                    for j in bins[str(i)]:
                        if cfv <= j:
                            break
                        nv += 1
                    fea[i] = str(nv)
                wfp.write(uid + '\t' + lbl + '\t' + '\t'.join(fea) + '\n')
                
    def one_hot_encode(self):
        inf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_lbl_bin'
        outf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_lbl_bin_ohe'
        binsf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/feature/bins'
        bins = {}
        with open(binsf, 'r') as fp:
            bins = json.loads(fp.readline().strip())
        with open(inf, 'r') as fp, open(outf, 'w') as wfp:
            for line in fp:
                linelist = line.strip().split('\t')
                uid = linelist[0]
                lbl = linelist[1]
                fea = linelist[2:]
                fea = [int(i) for i in fea]
                for i in range(len(fea)):
                    one_hot = [0]*(len(bins[str(i)])+1)
                    one_hot[fea[i]] = 1
                    fea[i] = one_hot
                new_fea = reduce(lambda x,y:x+y, fea)
                new_fea = [str(i) for i in new_fea]
                wfp.write(uid + '\t' + lbl + '\t' + '\t'.join(new_fea) + '\n')
    
    def add_lable2fea(self):
        lf = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/sample/s'
        fea = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea'
        ofp = '/home/liuxiaoliang/workspace/feature/working/experiment/20170104/test3/feature/fea_lbl'
        with open(lf, 'r') as f1, open(fea, 'r') as f2, open(ofp, 'w') as wfp:
            lable = {}
            for line in f1:
                linelist = line.strip().split('\t')
                lable[linelist[0]] = linelist[1]
            for line in f2:
                linelist = line.strip().split('\t')
                uid = linelist[0]
                if uid in lable:
                    re = [uid, lable[uid]] + linelist[1:]
                    wfp.write('\t'.join(re) + '\n')

    def format2svm(self):
        pass

    def sample(self):
        pass

    def roc(self):
        pass

    def auc(self):
        pass

if __name__ == '__main__':
    f = FeaEtl()
    #f.merge_fea()
    #f.add_lable2fea()
    #f.get_bin()
    #f.fea_bin()
    f.one_hot_encode()
