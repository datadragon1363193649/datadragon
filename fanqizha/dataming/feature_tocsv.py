# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import numpy as np
import offline_db_conf as dconf
reload(sys)

sys.setdefaultencoding('utf-8')

def traincsv(phnfilename):
    # 原始数据
    UID = dconf.uid
    TARGET = dconf.target
    s_file = dconf.config_path + dconf.featurename
    out_file = dconf.data_path + phnfilename + '.csv'
    f_file = dconf.data_path + phnfilename
    feaidlist = []
    feanamelist = []
    feaidlist.append(UID)
    feaidlist.append(TARGET)
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
    ff =[]
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            print len(linelist)
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    fdf.columns = feaidlist
    fdf.to_csv(out_file,index=False)
def testcsv(phnfilename):
    # 原始数据
    UID = dconf.uid
    TARGET = dconf.target
    s_file = dconf.config_path + dconf.featurename
    out_file = dconf.data_path + phnfilename + '.csv'
    f_file = dconf.data_path + phnfilename
    feaidlist = []
    feanamelist = []
    feaidlist.append(UID)
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
    ff = []
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    fdf.columns = feaidlist
    fdf=fdf.where(fdf.notnull(), '-1')
    fdf.to_csv(out_file, index=False)
if __name__ == '__main__':
    tat= sys.argv[1]
    input_file = sys.argv[2]
    if tat=='train':
        traincsv(input_file)
    if tat=='test':
        testcsv(input_file)


