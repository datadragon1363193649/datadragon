# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import numpy as np
import offline_db_conf as dconf
import json
reload(sys)

sys.setdefaultencoding('utf-8')
'''
对一些特征中某个值特别大，做变成类别型字段处理
处理标准：有这个值的是'0'，不是这个值的其他为'1'
'''
def train(phnfilename):
    s_file = dconf.config_path + dconf.featurename
    out_name='ind'+phnfilename
    out_file = dconf.data_path +out_name
    json_file=dconf.config_path +'classfeaname.json'
    wfp = open(json_file, 'w')
    f_file = dconf.data_path + phnfilename
    fnewd=pd.read_csv(f_file)
    columns = fnewd.columns.tolist()
    feadic={}
    cfdic={}
    with open(s_file,'r') as fp:
        for line in fp:
            linelist=line.strip().split(',')
            feadic[linelist[0]]=linelist[1]
    numv = 0.8 * len(fnewd)
    for col in columns[1:]:
        if col not in feadic:
            continue
        tvc = fnewd[col].value_counts()
        # print col,tvc
        # if tvc[tvc.index[0]] > numv9:
        #     print col
        #     del fnewd[col]
        #     continue
        if tvc[tvc.index[0]] > numv:
            fnewd[col] = fnewd[col].astype('string')
            fnewd[col][fnewd[col] == str(tvc.index[0])] = 'no'
            # testdfall['1'][testdfall['1'] != 1] = 0
            fnewd[col][fnewd[col] != 'no'] = '1'
            fnewd[col][fnewd[col] == 'no'] = '0'
            fnewd[col] = fnewd[col].astype('float64')
            cfdic[feadic[col]]=tvc.index[0]
            # wfp.write(feadic[col]+','+str(tvc.index[0])+'\n')
    json.dump(cfdic,wfp)
    fnewd.to_csv(out_file,index=False)
    return out_name
if __name__ == '__main__':
    i_file=sys.argv[1]
    train(i_file)