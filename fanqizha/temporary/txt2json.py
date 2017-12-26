# -*- encoding: utf-8 -*-
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
outfile='/Users/ufenqi/Downloads/featurename.json'
s_file='/Users/ufenqi/Downloads/featurename'
wfp=open(outfile,'w')
fwd={}
with open(s_file,'r') as fp:
    for line in fp:
        linelist=line.strip().split(',')
        fwd[linelist[0]]=linelist[1]
