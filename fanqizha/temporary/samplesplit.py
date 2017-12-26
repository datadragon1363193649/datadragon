# -*- encoding: utf-8 -*-
import sys
import numpy as np
reload(sys)
sys.setdefaultencoding('utf-8')


def pro_fea_split( k):
    """
    蓄水池算法, k为百分比
    """
    # if k > 100: exit(0)
    pro_file = '/Users/ufenqi/Documents/dataming/bank/data/score_730_910_zheng'
    output_dir = '/Users/ufenqi/Documents/dataming/bank/data/'
    s_1 = output_dir + 'score_730_910_zheng_train'
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
if __name__ == '__main__':
    pro_fea_split(10000)