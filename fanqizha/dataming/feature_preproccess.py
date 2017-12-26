# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import numpy as np
import offline_db_conf as dconf
reload(sys)

sys.setdefaultencoding('utf-8')

def traincsv(phnfilename):
    # 原始数据
    s_file = dconf.config_path + dconf.featurename
    out_file = dconf.data_path + phnfilename + '.csv'
    f_file = dconf.data_path + phnfilename
    # f_file = '/Users/ufenqi/Documents/dataming/base1/data/phnall501102'
    # s_file = '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurename_online'
    feaidlist = []
    feanamelist = []
    feaidlist.append('0')
    feaidlist.append('1')
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
    columns = fdf.columns.tolist()
    fnewd = fdf[columns]
    # for col in columns[1:]:
    #     fnewd[col] = fnewd[col].astype('float64')

    fnewd.to_csv(out_file,index=False)
    return out_file
def testcsv(phnfilename):
    # 原始数据
    s_file = dconf.config_path + dconf.featurename
    out_file = dconf.data_path + phnfilename + '.csv'
    f_file = dconf.data_path + phnfilename
    # f_file = '/Users/ufenqi/Documents/dataming/base1/data/phnall501101'
    # s_file = '/Users/ufenqi/Documents/dataming/base1/config/featurename_online'
    feaidlist = []
    feanamelist = []
    feaidlist.append('0')
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
    # print len(feaidlist)
    # print feaidlist
    ff = []
    with open(f_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            # if len(linelist)<75:
            #     print linelist[0]
            #     continue
            # print linelist[0],len(linelist)
            ff.append(linelist)
    fdf = pd.DataFrame(ff)
    # print fdf.head()
    fdf.columns = feaidlist
    columns = fdf.columns.tolist()
    print len(columns)
    print columns
    fnewd = fdf[columns]

    # for col in columns[1:]:
    #
    #     fnewd[col] = fnewd[col].astype('float64')
    fnewd=fnewd.where(fnewd.notnull(), '-1')
    # print fnewd['97']
    fnewd.to_csv(out_file, index=False)
    return out_file
if __name__ == '__main__':
    tat= sys.argv[1]
    input_file = sys.argv[2]
    if tat=='train':
        traincsv(input_file)
    if tat=='test':
        testcsv(input_file)


