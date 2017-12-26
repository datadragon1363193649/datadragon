# -*- cocoding: utf-8 -*-
from sklearn.cluster import KMeans
from sklearn.cluster import SpectralClustering
from  sklearn.cluster import AgglomerativeClustering
from sklearn.cluster import  AffinityPropagation
from sklearn.cluster import DBSCAN
import offline_db_conf as dconf
import sys
import pandas as pd
import matplotlib.pylab as plt
import json
from sklearn.externals import joblib
from scipy.stats import ks_2samp
reload(sys)
def kmeans(phnfilename,num,mark):
    s_file = dconf.config_path + dconf.featurename
    weight_json_file=dconf.config_path+'weight_610.json'
    f_file = dconf.data_path + phnfilename
    model_file = dconf.model_path + 'kmeanmodel_positive'
    y_file=dconf.data_path + 'y_phn_7_15'
    g_file=dconf.data_path + 'g_phn_7_15'
    all_file = dconf.data_path + 'score_td_715'
    out_file=dconf.data_path + "score"+phnfilename
    wfp=open(out_file,'w')
    tdf = pd.read_csv(f_file)
    feaidlist = []
    feaidlist1 = []
    feanamedic = {}
    # feaidlist.append('0')
    # feaidlist.append('1')
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist1.append(linelist[0])
            feanamedic[linelist[0]]=linelist[1]
    if mark=='all':
        feaidlist=feaidlist1
    else:
        with open(weight_json_file, 'r') as fp:
            c = fp.readline()
            c = json.loads(c)
            for fea in feaidlist1:
                feaname=feanamedic[fea]
                if c[feaname]>0.1 :
                    feaidlist.append(fea)


    # feaidlist=['6','7','22','34','40','42','43','104','105','106','107','108','109']
    # feaidlist = ['19', '20', '23', '24', '25', '26']
    print feaidlist
    ydic={}
    # tdf['1']=tdf['0']
    with open(y_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            # tdf['1'][tdf['0']==int(linelist[0])]=0
            ydic[linelist[0]]=0
    gdic={}
    with open(g_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split('\t')
            # tdf['1'][tdf['0'] == int(linelist[0])] =1
            gdic[linelist[0]] = 0
    alldic = {}
    with open(all_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            # tdf['1'][tdf['0'] == int(linelist[0])] =1
            # print linelist
            alldic[linelist[0]] = linelist

    # print tdf['1'][tdf['36t'] > 0].value_counts()
    # tdf.to_csv(dconf.data_path + 'target' + phnfilename, index=False)
    phnlist=ydic.keys()+gdic.keys()

    # tdf=tdf[tdf['0'] in phnlist]
    print feaidlist
    kmean=joblib.load(model_file)
    # kmean = KMeans(n_clusters=num).fit(tdf[feaidlist])
    userid = tdf['0'].values.tolist()
    lab=kmean.labels_
    # joblib.dump(kmean, model_file)
    # joblib.dump(logm, model_file)
    # usery=testdfall['1'].values.tolist()
    # # #

    uscoredic={}
    # for ui in range(0, len(userid)):
    #     phn=str(userid[ui])
    #     # uscoredic[str(userid[ui])]=lab[ui]
    #     if phn in ydic:
    #         ydic[phn]=lab[ui]
    #     if phn in gdic:
    #         gdic[phn]=lab[ui]
    #     wfp.write(str(userid[ui]) + ',' + str(lab[ui]) + '\n')
    scorelist=[]
    for ui in range(0, len(userid)):
        phn=str(userid[ui])
        # uscoredic[str(userid[ui])]=lab[ui]
        # print phn
        if phn in alldic:
            alldic[phn].append(str(lab[ui]))
            # print alldic[phn]
            scorelist.append(alldic[phn])
    scoredf=pd.DataFrame(scorelist,columns=['phn','daytime','myscore','tdscore','overday','kmeansclass'])
    scoredf['overday']=scoredf['overday'].astype(float)
    scoredf['tdscore'] = scoredf['tdscore'].astype(float)
    scoredf['myscore'] = scoredf['myscore'].astype(float)

    scoredf = scoredf[scoredf['overday'] > -1]

    print '逾期用户'
    print scoredf['kmeansclass'][scoredf['overday']>59].value_counts()
    print '好用户'
    print scoredf['kmeansclass'][scoredf['overday']<=29].value_counts()
    scoredf['overday'][(scoredf['overday'] > -1) & (scoredf['overday'] < 30)] = 1
    scoredf['overday'][scoredf['overday'] != 1] = 0
    get_ks = lambda y_pred, y_true: ks_2samp(y_pred[y_true == 1], y_pred[y_true != 1]).statistic

    print 'ks值', get_ks(scoredf['tdscore'], scoredf['overday'])
    # scoredf.to_csv(dconf.data_path + 'kmeans' + phnfilename, index=False)
        # if phn in gdic:
        #     gdic[phn]=lab[ui]
        # wfp.write(str(userid[ui]) + ',' + str(lab[ui]) + '\n')



    # r1 = pd.Series(kmeans.labels_).value_counts()
    # r2 = pd.DataFrame(kmeans.cluster_centers_)
    # r = pd.concat([r2, r1], axis=1)  # 横向连接(0是纵向), 得到聚类中心对应的类别下的数目
    # r.columns = feaidlist + [u'类别数目']  # 重命名表头
    # print(r)
    # print '逾期用户'
    # print pd.Series(ydic.values()).value_counts()
    # print '好用户'
    # print pd.Series(gdic.values()).value_counts()
    # lab=kmeans.labels_
    # plt.figure(1)
    # n, bins, patches = plt.hist(kmeans.labels_, num, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist_ydic')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()

    # plt.figure(1)
    # n, bins, patches = plt.hist(gdic.values(), 50, normed=1, facecolor='g', alpha=0.75)
    # plt.xlabel('hist_gdic')
    # plt.ylabel('Probability')
    # plt.title('Histogram')
    # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
    # plt.grid(True)
    # plt.show()
if __name__ == '__main__':
    # tat = sys.argv[1]
    tat='score_td_7152.csv'
    num = 50
    mark='all'
    kmeans(tat,num,mark)