# -*- cocoding: utf-8 -*-
import sys
import pandas as pd
import seaborn as sns
import json
import offline_db_conf as dconf
reload(sys)
import numpy as np
sys.setdefaultencoding('utf-8')

"""
临时处理：把通讯录特征全部为0
"""
def test(dfname):
    UID = dconf.uid
    TARGET = dconf.target
    test_file = dconf.data_path + dfname
    out_name = 'zero' + dfname
    out_file = dconf.data_path + out_name
    s_file = dconf.config_path + dconf.featurename
    testdf = pd.read_csv(test_file)
    feanamedic = {}
    if TARGET in testdf.columns.tolist():
        flist = [UID, TARGET]
    else:
        flist = [UID]
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            flist.append(linelist[0])
            feanamedic[linelist[0]] = linelist[1]
    testdf = testdf.round(10)

    columnslist = testdf.columns.tolist()
    columnslist.remove(UID)
    if TARGET in columnslist:
        columnslist.remove(TARGET)
    for col in columnslist:
        if col in flist or col in ['2','63','43','44','45','47']:
            continue
        testvll = list(set(testdf[col].values.tolist()))

        dfl=[0 for i in range(0,len(testvll))]
        testdf[col].replace(testvll, dfl, inplace=True)
    testdf.to_csv(out_file, index=False)
    return out_name
if __name__ == '__main__':
    input_file = sys.argv[1]
    test(input_file)