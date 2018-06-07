# -*- encoding: utf-8 -*-
import sys
import numpy as np
reload(sys)
sys.setdefaultencoding('utf-8')
'''
样本筛选
'''

def pro_fea_split( k):
    """
    蓄水池算法, k为百分比
    """
    # if k > 100: exit(0)
    pro_file = '/Users/ufenqi/Downloads/train_zheng_all'
    output_dir = '/Users/ufenqi/Downloads/'
    s_1 = output_dir + 'train_zheng_sample'
    s = []
    re = []
    with open(pro_file, 'r') as rfp:
        for line in rfp:
            s.append(line)
    # k = int(0.01 * k * len(s))
    re = s[:k]
    for i, e in enumerate(s[k:]):
        r = np.random.randint(0, k + i + 1)
        if r <= k - 1:
            re[r] = e
    with open(s_1, 'w') as wfp1:
        for l in s:
            if l in re:
                wfp1.write(l)
# stacking 子集抽取
def split_n_subset(k):
    def split(all,ana_num,out):
        re = []
        samplelist=[]
        re = all[:ana_num]
        for i, e in enumerate(all[ana_num:]):
            r = np.random.randint(0, ana_num + i + 1)
            if r <= ana_num - 1:
                re[r] = e
        with open(out, 'w') as wfp1:
            for l in all:
                if l in re:
                    samplelist.append(l)
                    wfp1.write(l)
        wfp1.close()
        return samplelist
    pro_file = '/Users/ufenqi/Documents/dataming/integration_new/data/train_uid_mar2'
    output_dir = '/Users/ufenqi/Documents/dataming/integration_new/data/'
    s_1 = output_dir + 'train_uid_sample'
    s = []
    with open(pro_file, 'r') as rfp:
        for line in rfp:
            s.append(line)
    len_all=len(s)
    ana_num=len_all/k
    print ana_num
    for ki in range(k-1):
        sample_all=split(s,ana_num,s_1+str(ki))
        for sa in sample_all:
            if sa in s:
                s.remove(sa)
        print len(s)
    with open(s_1+str(k-1), 'w') as wfp1:
        for l in s:
            wfp1.write(l)
    wfp1.close()
if __name__ == '__main__':
    # pro_fea_split(15000)
    split_n_subset(4)