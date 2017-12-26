#! /usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import sys

class OnlineEval(object):
    def __init__(self):
        pass
        
    def join(self):
        f1 = '/home/liuxiaoliang/workspace/feature/working/online_eval/wc_20161115_20161220'
        f2 = '/home/liuxiaoliang/workspace/feature/working/online_eval/td_20161115_20161220'
        f3 = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk_1/jk_1'
        f4 = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk_2/jk_2'
        f5 = '/home/liuxiaoliang/workspace/feature/working/online_eval/jk_3/jk_3'
        of1 = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/wc'
        of2 = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/td'
        of3 = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/jk_1'
        of4 = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/jk_2'
        of5 = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/jk_3'
        u1_info = set()
        u2_info = set()
        u3_info = set()
        u4_info = set()
        u5_info = set()
        with open(f1, 'r') as fp1, \
             open(f2, 'r') as fp2, \
             open(f3, 'r') as fp3, \
             open(f4, 'r') as fp4, \
             open(f5, 'r') as fp5:
            for line in fp1:
                uid = line.split('\t')[0]
                u1_info.add(uid)
            for line in fp2:
                uid = line.split('\t')[0]
                u2_info.add(uid)
            for line in fp3:
                uid = line.split('\t')[0]
                u3_info.add(uid)
            for line in fp4:
                uid = line.split('\t')[0]
                u4_info.add(uid)
            for line in fp5:
                uid = line.split('\t')[0]
                u5_info.add(uid)
            
        uid_list = u1_info & u2_info & u3_info & u4_info & u5_info
        with open(f1, 'r') as fp1, \
             open(f2, 'r') as fp2, \
             open(f3, 'r') as fp3, \
             open(f4, 'r') as fp4, \
             open(f5, 'r') as fp5, \
             open(of1, 'w') as wfp1, \
             open(of2, 'w') as wfp2, \
             open(of3, 'w') as wfp3, \
             open(of4, 'w') as wfp4, \
             open(of5, 'w') as wfp5:
            for line in fp1:
                uid = line.split('\t')[0]
                if uid in uid_list:
                    wfp1.write(line)
            for line in fp2:
                uid = line.strip().split('\t')[0]
                if uid in uid_list:
                    wfp2.write(line)
            for line in fp3:
                uid = line.strip().split('\t')[0]
                if uid in uid_list:
                    wfp3.write(line)
            for line in fp4:
                uid = line.strip().split('\t')[0]
                if uid in uid_list:
                    wfp4.write(line)
            for line in fp5:
                uid = line.strip().split('\t')[0]
                if uid in uid_list:
                    wfp5.write(line)


    def pass_eval(self):
        # tongdun 分值越小越好 step = 1.5
        # wecash  分值越大越好 step = 5
        # jk 分值越大越好 step = 3
        pro_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/pass/jk_3'  # uid score status
        output_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/pass/jk_3_rate'
        e = []
        with open(pro_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split('\t')
                uid, score, status = linelist
                if int(status) >= 0:
                    e.append((uid, float(score), int(status)))
        e.sort(key = lambda x:x[1])
        # wc
        e_count = len(e)
        with open(output_file, 'w') as fp:
            beg = e[-1][1]
            _pass_count = 0
            _odd_count = 0
            for i in range(e_count-1, -1, -1):
                if e[i][1] >= beg - 3:
                    _pass_count += 1
                    if e[i][2] >= 3:
                        _odd_count += 1
                else:
                    beg = e[i][1]
                    pr = 0
                    odr = 0
                    if _pass_count > 0:
                        pr = 1.0*_pass_count/e_count
                        odr = 1.0*_odd_count/_pass_count
                    fp.write(str(pr) + '\t' + str(odr) + '\n')
                    _pass_count += 1
                    if e[i][2] >= 3:
                        _odd_count += 1
                    #print _pass_count, _odd_count

            pr = 0
            odr = 0
            if _pass_count > 0:
                pr = 1.0*_pass_count/e_count
                odr = 1.0*_odd_count/_pass_count
            fp.write(str(pr) + '\t' + str(odr) + '\n')
        
                
    def all_eval(self):
        """
        output: pass_rate base_count base_pro1_count base_pro2_count ...
        """
        base_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/all/wc'  # wecash
        pro1_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/all/td' # tongdun
        pro2_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/all/jk_1' # jk
        pro3_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/all/jk_2'
        pro4_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/all/jk_3'
        output_file = '/home/liuxiaoliang/workspace/feature/working/online_eval/eval/all/pm'
        pass_rate = [i for i in range(95, 0, -5)]
        with open(base_file, 'r') as fp1, \
             open(pro1_file, 'r') as fp2, \
             open(pro2_file, 'r') as fp3, \
             open(pro3_file, 'r') as fp4, \
             open(pro4_file, 'r') as fp5, \
             open(output_file, 'w') as wfp:
            base = []
            pro1 = []
            pro2 = []
            pro3 = []
            pro4 = []
            for line in fp1:
                linelist = line.strip().split('\t')
                base.append([linelist[0], float(linelist[1])])
            for line in fp2: # 
                linelist = line.strip().split('\t')
                pro1.append([linelist[0], -1*float(linelist[1])])
            for line in fp3:
                linelist = line.strip().split('\t')
                pro2.append([linelist[0], float(linelist[1])])
            for line in fp4:
                linelist = line.strip().split('\t')
                pro3.append([linelist[0], float(linelist[1])])
            for line in fp5:
                linelist = line.strip().split('\t')
                pro4.append([linelist[0], float(linelist[1])])
            
            base.sort(key=lambda x:x[1])
            pro1.sort(key=lambda x:x[1], reverse=True)
            pro2.sort(key=lambda x:x[1])
            pro3.sort(key=lambda x:x[1])
            pro4.sort(key=lambda x:x[1])
            bc = len(base)
            p1c = len(pro1)
            p2c = len(pro2)
            p3c = len(pro3)
            p4c = len(pro4)
            for i in pass_rate:
                b_index = int(bc*i*0.01)
                p1_index = int(p1c*i*0.01)
                p2_index = int(p2c*i*0.01)
                p3_index = int(p3c*i*0.01)
                p4_index = int(p4c*i*0.01)
                b_part = set([j[0] for j in base[b_index:]])
                p1_part = set([j[0] for j in pro1[p1_index:]])
                p2_part = set([j[0] for j in pro2[p2_index:]])
                p3_part = set([j[0] for j in pro3[p3_index:]])
                p4_part = set([j[0] for j in pro4[p4_index:]])
                m1 = len(b_part & p1_part)/100.0
                m2 = len(b_part & p2_part)/100.0
                m3 = len(b_part & p3_part)/100.0
                m4 = len(b_part & p4_part)/100.0
                re = [100-i, len(b_part)/100.0, m1, m2, m3, m4]
                re = [str(i) for i in re]
                wfp.write('\t'.join(re) + '\n')
                

if __name__ == '__main__':
    oe = OnlineEval()
    #oe.join()
    oe.pass_eval()
    #oe.all_eval()
