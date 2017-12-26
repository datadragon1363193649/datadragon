# -*- cocoding: utf-8 -*-
from sklearn.pipeline import Pipeline
# from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import (RandomTreesEmbedding, RandomForestClassifier,
                              GradientBoostingClassifier)
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
import pandas as pd
from sklearn import cross_validation, metrics
import json
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import seaborn as sns
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import roc_curve
from sklearn.externals import joblib
#lr是一个LogisticRegression模型
# joblib.dump(lr, 'lr.model')
# lr = joblib.load('lr.model')



#
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainnumbercnt.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testnumbercnt.csv')

# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_code2.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code2.csv')
#
# traindfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
# testdfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcoderealrenzheng.csv')

traindfall=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/test/codenordeltraindata_addfu.csv')
testdfall=pd.read_csv('/Users/ufenqi/Documents/dataming/base1/test/codenordelscore_710_addfu.csv')
# testdfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code4-3_10.csv')
# testdfall['1'].replace([1.0,0.0], [0.0,1.0], inplace=True)
print traindfall['1'].value_counts()
# print testdfall['1'].value_counts()
# testdfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode.csv')
# testdfall=testdfall[testdfall['1']>-1]
# testdfall['1'][(testdfall['1'] > -1) & (testdfall['1'] < 30)] = 1
# testdfall['1'][testdfall['1'] != 1] = 0
# print testdfall['1'].value_counts()
# traindfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
# testdfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcoderealrenzheng.csv')
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodeandnum.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcodeandnum.csv')
use_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/callfeaturename'
name_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/featurecallcode'
noridlist=[]
nordic={}
with open(name_file, 'r') as fp:
    for line in fp:
        norfealist = line.strip().split(',')
        noridlist.append(norfealist[0])
        nordic[norfealist[0]] = norfealist[1]
# use_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/bankfeaturename'
linelist=[]
with open(use_file, 'r') as fp:
    for line in fp:
        linelist = line.strip().split(',')
linelist.remove('27lognorfen10')
linelist.remove('18lognorfen10')
linelist1=[]
for lin in linelist:
    if 'fen' not in lin:
        linelist1.append(lin)
# print linelist
# sns.boxplot(traindf['73lognor'])
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainfenxiang.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testfenxiang.csv')
target='1'
IDcol='0'
# print testdf[target].value_counts()
predictors = [x for x in traindfall.columns if x not in [target, IDcol] ]
# for pre in predictors:
#     if '63'in pre:
predictors=linelist
print len(predictors)
print predictors
# predictors.remove('63')
# predictors.remove('73lognor')
# predictors.remove('72lognor')
# # predictors.remove('67lognor')
# # predictors.remove('71lognor')
# # # predictors.remove('75lognor')
# # # predictors.remove('70lognor')
# # # predictors.remove('74lognor')
# predictors.remove('39lognor')
# predictors.remove('39lognorfen')
# predictors.remove('40lognor')
# predictors.remove('40lognorfen')
# # # predictors.remove('41lognor')
# # # predictors.remove('41lognorfen')
# predictors.remove('42lognor')
# predictors.remove('42lognorfen')
# testpredictors=[]
# for pre in predictors:
#     testpredictors.append(pre.split('_')[0])

# gbm1 = GradientBoostingClassifier()
#
# param_test1 = {'n_estimators':range(121,181,10)}
# gsearch1 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, min_samples_split=300,
#                                   min_samples_leaf=20,max_depth=8,max_features='sqrt', subsample=0.8,random_state=10),
#                        param_grid = param_test1, scoring='roc_auc',iid=False,cv=4)
# gsearch1.fit(traindfall[predictors],traindfall[target])
# print gsearch1.grid_scores_, gsearch1.best_params_, gsearch1.best_score_

#
# param_test2 = {'max_depth':range(3,14,2), 'min_samples_split':range(100,801,200)}
# gsearch2 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=160, min_samples_leaf=20,
#       max_features='sqrt', subsample=0.8, random_state=10),
#    param_grid = param_test2, scoring='roc_auc',iid=False, cv=4)
# gsearch2.fit(traindfall[predictors],traindfall[target])
# print gsearch2.grid_scores_, gsearch2.best_params_, gsearch2.best_score_


# param_test3 = {'min_samples_split':range(100,1900,200), 'min_samples_leaf':range(60,101,10)}
# gsearch3 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=160,max_depth=9,
#                                      max_features='sqrt', subsample=0.8, random_state=10),
#                        param_grid = param_test3, scoring='roc_auc',iid=False, cv=3)
# gsearch3.fit(traindfall[predictors],traindfall[target])
# print gsearch3.grid_scores_, gsearch3.best_params_, gsearch3.best_score_

# param_test4 = {'max_features':range(7,20,2)}
# gsearch4 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=150,max_depth=5, min_samples_leaf =70,
#                min_samples_split =1300, subsample=0.8, random_state=10),
#                        param_grid = param_test4, scoring='roc_auc',iid=False, cv=5)
# gsearch4.fit(traindfall[predictors],traindfall[target])
# print  gsearch4.grid_scores_, gsearch4.best_params_, gsearch4.best_score_

# #
# gbm1 = GradientBoostingClassifier(learning_rate=0.2, n_estimators=75,max_depth=7, min_samples_leaf =70,
#                min_samples_split =1100, max_features='sqrt',subsample=0.7,  random_state=10)
# # # gbm1 = GradientBoostingClassifier()
# # # gbm1 = RandomForestClassifier(max_depth=3, n_estimators=150)
# #
# gbm1.fit(traindfall[predictors],traindfall[target])
# y_pred = gbm1.predict(testdfall[predictors])
# y_predprob = gbm1.predict_proba(testdfall[predictors])[:,1]
#
# # print gbm1.predict_proba(testdfall[predictors])
# print 'gbdt'
# print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_pred)
# print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprob)
# fpr_grd_lm, tpr_grd_lm, _ = roc_curve(testdfall[target], y_predprob)
# joblib.dump(gbm1, '/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/model/call.model')
#

# logm = LogisticRegression(C=1.0,penalty='l1',solver='liblinear',multi_class='ovr')
logm = LogisticRegression(C=1.0,penalty='l1',solver='liblinear',multi_class='ovr')
logm.fit(traindfall[predictors],traindfall[target])
# y_predl = logm.predict(testdfall[predictors])
# y_predprobl = logm.predict_proba(testdfall[predictors])[:,1]
# print y_predprobl
outweight_file='/Users/ufenqi/Documents/dataming/base1/config/featurenameAndWeight7.json'
wfp=open(outweight_file,'w')
weightlist=logm.coef_[0]
# # print weightlist
featureweidc={}
print nordic
for i in range(0,len(predictors)):
    print predictors[i],nordic[predictors[i]],weightlist[i]
    featureweidc[nordic[predictors[i]]]=round(weightlist[i],10)
featureweidc['bias']=round(logm.intercept_[0],10)
json.dump(featureweidc,wfp)
joblib.dump(logm, '/Users/ufenqi/Documents/dataming/base1/model/lr7.model')
# for i in range(0,len(y_predl)):
#     print y_predl[i],y_predprobl[i]
# print gbm1.predict_proba(testdfall[predictors])
# print 'Logistic'
# print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_predl)
# print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprobl)
# fpr_grd_lml, tpr_grd_lml, _ = roc_curve(testdfall[target], y_predprobl)
# plt.figure(1)
# n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
# plt.xlabel('hist')
# plt.ylabel('Probability')
# plt.title('Histogram')
# #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
# plt.grid(True)
# plt.show()
# plt.figure(1)
# plt.plot([0, 1], [0, 1], 'k--')
#
# plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT')
# plt.plot(fpr_grd_lml, tpr_grd_lml)
# plt.xlabel('False positive rate')
# plt.ylabel('True positive rate')
# plt.legend(loc='besergsegegegegt')



# plt.figure(2)
# n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
# plt.xlabel('hist')
# plt.ylabel('Probability')
# plt.title('Histogram')
# #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
# plt.grid(True)
# plt.show()



# weightlist=range(1,10)
# userid =testdfall[target].values.tolist()
# # for wei in weightlist:
# #     uy=[]
# #     ufen=[]
# #     # welist=[]
# #     for uid in range(0,len(y_predprobl)):
# #         uy.append(userid[uid])
# #         fen=y_predprob[uid]*wei*0.1+y_predprobl[uid]*(1-wei*0.1)
# #         ufen.append(fen)
# #         # welist.append(wecashdic[uid])
# #     fpr_grd_lm, tpr_grd_lm, _ = roc_curve(uy, ufen)
# #     # fpr_grd_lmw, tpr_grd_lmw, _ = roc_curve(uy, welist, pos_label=0)
# #     print 'call权重',wei
# #     print "AUC Score (Train): %f" % metrics.roc_auc_score(uy, ufen)
# #     # plt.figure(1)
# #     # plt.plot([0, 1], [0, 1], 'k--')
# #     # # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
# #     # plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT')
# #     # plt.plot(fpr_grd_lmw, tpr_grd_lmw, label='wecash')
# #     # plt.xlabel('False positive rate')
# #     # plt.ylabel('True positive rate')
# #     # plt.legend(loc='best')
# #     # plt.show()
# #     # plt.figure(1)
# #     # n, bins, patches = plt.hist(welist, 100, normed=1, facecolor='g', alpha=0.75)
# #     # plt.xlabel('hist')
# #     # plt.ylabel('Probability')
# #     # plt.title('Histogram')
# #     # # plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
# #     # plt.grid(True)
# #     # plt.show()
# #
# #     plt.figure(2)
# #     n,bins,patches=plt.hist(ufen,100,normed=1,facecolor='g',alpha=0.75)
# #     plt.xlabel('hist')
# #     plt.ylabel('Probability')
# #     plt.title('Histogram')
# #     #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
# #     plt.grid(True)
# #     # plt.show()
# out_file='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/11_15_test/usercallfen_15'
# wfp=open(out_file,'w')
# # # print y_predprob
# userid =testdfall['0'].values.tolist()
# usery=testdfall['1'].values.tolist()
# # #

# for ui in range(0,len(userid)):
#     wfp.write(str(userid[ui])+','+str(y_predprobl[ui])+'\n')
# y_predprobl.sort()
# for i in range(0,10):
#     num=0
#     for ui in range(0,len(userid)):
#         index1 = int(0.1 * i * len(y_predprobl))
#         if y_predprobl[ui]>(y_predprobl[index1]):
#             if usery[ui]==1.0:
#                 num+=1
#         else:
#             if usery[ui] == 0.0:
#                 num+=1
#     print 'biao:',i*0.1,'acc:',1.0*num/len(userid)
#
# index1=int(0.4*len(y_predprobl))
# print y_predprobl[index1]
# fpr_grd_lm1, tpr_grd_lm1, _ = roc_curve(testdfall[target], y_predprob)
# n_estimator=150
# grd = GradientBoostingClassifier(n_estimators=n_estimator)
# grd_enc = OneHotEncoder()
# grd_lm = LogisticRegression()
# grd.fit(traindfall[predictors],traindfall[target])
# grd_enc.fit(grd.apply(traindfall[predictors])[:, :, 0])
# print grd_enc
# grd_lm.fit(grd_enc.transform(grd.apply(traindfall[predictors])[:, :, 0]), traindfall[target])
#
# y_pred_grd_lm = grd_lm.predict_proba(
#     grd_enc.transform(grd.apply(testdfall[predictors])[:, :, 0]))[:, 1]
#
# print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_pred_grd_lm)
# fpr_grd_lm, tpr_grd_lm, _ = roc_curve(testdfall[target], y_predprob)
#
#
#
#
# plt.figure(1)
# plt.plot([0, 1], [0, 1], 'k--')
#
# plt.plot(fpr_grd_lm, tpr_grd_lm, label='GBT + LR')
# plt.xlabel('False positive rate')
# plt.ylabel('True positive rate')
# plt.legend(loc='best')
# plt.show()
# # #
# feat_imp = pd.Series(gbm1.feature_importances_, predictors).sort_values(ascending=False)
# for i in range(100,180,10):
#     precollist=feat_imp[0:i].index.tolist()
#     gbm1.fit(traindfall[precollist],traindfall[target])
#
#     y_pred = gbm1.predict(testdfall[precollist])
#     y_predprob = gbm1.predict_proba(testdfall[precollist])[:,1]
#     print 'feature',i
#     print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_pred)
#     print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprob)






#
# feat_imp = pd.Series(gbm1.feature_importances_, predictors).sort_values(ascending=False)
# precollistnor=feat_imp[0:60].index.tolist()
# precollist+=precollistnor
# precollist=list(set(precollist))
# print precollist
# print len(precollist)
# traindfall1=traindfall[precollist]
#
# traindfall1.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfall.csv',index=False)
# # predictors.remove('73lognor')
# # predictors.remove('72lognor')
#
#
# gbm1.fit(traindfall[predictors],traindfall[target])
#
#
# # print gbm1.feature_importances_
# y_pred = gbm1.predict(testdfall[predictors])
# y_predprob = gbm1.predict_proba(testdfall[predictors])[:,1]
# tr=testdfall[target].values.tolist()
# tu=testdfall[IDcol].values.tolist()
# # for i in range(0,len(y_predprob)):
# #     wfp.write(str(y_predprob[i])+'\t'+str(y_pred[i])+'\t'+str(tr[i])+'\t'+str(tu[i])+'\n')
# #     print y_predprob[i],y_pred[i],tr[i],tu[i]
# # wfp.close()
# print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_pred)
# print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprob)
#
# feat_imp[0:50].plot(kind='bar', title='Feature Importances')
# plt.ylabel('Feature Importance Score')
# plt.show()


# plt.figure(1)
# n,bins,patches=plt.hist(y_predprobl,100,normed=1,facecolor='g',alpha=0.75)
# plt.xlabel('hist')
# plt.ylabel('Probability')
# plt.title('Histogram')
# #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
# plt.grid(True)
# plt.show()


#
# #
# plt.figure(2)
# n,bins,patches=plt.hist(y_pred_grd_lm,100,normed=1,facecolor='g',alpha=0.75)
# plt.xlabel('hist')
# plt.ylabel('Probability')
# plt.title('Histogram')
# #plt.text(60,.025, r'$\mu=100,\ \sigma=15$')
# plt.grid(True)
# plt.show()



# param_test5 = {'subsample':[0.6,0.7,0.75,0.8,0.85,0.9]}
# gsearch4 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=70,max_depth=7, min_samples_leaf =70,
#                min_samples_split =800, max_features=17, random_state=10),
#                        param_grid = param_test5, scoring='roc_auc',iid=False, cv=3)
# gsearch4.fit(train_feat,train_id)
# print gsearch4.grid_scores_, gsearch4.best_params_, gsearch4.best_score_
# print train_feat.shape,train_id.shape,test_feat.shape,test_id.shape
# param_test3 = {'min_samples_split':range(800,1900,200), 'min_samples_leaf':range(60,101,10)}
# gbdt = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=90,max_depth=7,
#                                      max_features='sqrt', subsample=0.8, random_state=10),
#                        param_grid = param_test3, scoring='roc_auc',iid=False, cv=5)


# grd_lm.fit(train_feat,train_id)
# gbdt.fit(train_feat,train_id)

# print gbdt.grid_scores_, gbdt.best_params_, gbdt.best_score_
# print gbdt
# pred=gbdt.predict(test_feat)

# pred=gbdt.predict(test_feat)

# pred.sort()
# print pred
# print len(pred)
# joblib.dump(gbdt, 'gbdtmodel')
# y_predprob = gbdt.predict_proba(test_feat)[:,1]
# num=0
# for i in range(0,len(test_id)):
#     if pred[i]>0.0:
#         num+=1
# print num
# y_predprob.sort()
# print y_predprob
# print "Accuracy : %.4g" % metrics.accuracy_score(test_id, pred)
# print "AUC Score (Train): %f" % metrics.roc_auc_score(test_id, y_predprob)
# grid = GridSearchCV(pipeline, cv=3, param_grid=parameters)
# grid.fit(train_feat, train_id)

