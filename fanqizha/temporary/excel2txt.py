# -*- encoding: utf-8 -*-
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
bankdf=pd.read_excel('/Users/ufenqi/Desktop/bank_yellowpage.xlsx')
bankdf.columns=['source', 'phone']
bankdf['source']=bankdf['source'].astype('string')
bankdf['phone']=bankdf['phone'].astype('string')
bankmobile = bankdf['phone'].values.tolist()
bankname = bankdf['source'].values.tolist()
outfile='/Users/ufenqi/Desktop/bank_yellowpage'
wfp=open(outfile,'w')
for i in range(0,len(bankname)):
    # print bankname[i],bankmobile[i]
    wfp.write(bankname[i].encode('utf-8')+","+bankmobile[i].encode('utf-8')+'\n')
