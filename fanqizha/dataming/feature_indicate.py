# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import numpy as np
import offline_db_conf as dconf
import json
reload(sys)

sys.setdefaultencoding('utf-8')
def indicate(phnfilename):
    s_file = dconf.config_path + dconf.featurename
    out_file = dconf.data_path +'ind'+phnfilename
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
if __name__ == '__main__':
    i_file=sys.argv[1]
    indicate(i_file)