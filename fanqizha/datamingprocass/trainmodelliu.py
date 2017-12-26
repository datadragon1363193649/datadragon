# -*- cocoding: utf-8 -*-
from sklearn.pipeline import Pipeline
# from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.externals import joblib
import pandas as pd
from sklearn import cross_validation, metrics
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import seaborn as sns

#
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainnumbercnt.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testnumbercnt.csv')

# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_code2.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code2.csv')
#
# traindfnor=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnor.csv')
# testdfnor=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdfnor.csv')

traindfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
testdfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcode.csv')
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodeandnum.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcodeandnum.csv')

# sns.boxplot(traindf['73lognor'])
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/trainfenxiang.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/fanqizhaallmax/testfenxiang.csv')
target='1'
IDcol='0'
# print testdf[target].value_counts()

predictors = [x for x in traindfall.columns if x not in [target, IDcol] ]
predictors.remove('73lognor')
predictors.remove('72lognor')
# predictors.remove('67lognor')
# predictors.remove('71lognor')
# predictors.remove('75lognor')
# predictors.remove('70lognor')
# predictors.remove('74lognor')
print predictors
print len(predictors)

testpredictors=[]
for pre in predictors:
    testpredictors.append(pre.split('_')[0])

gbm1 = GradientBoostingClassifier()


gbm1.fit(traindfall[predictors],traindfall[target])


# print gbm1.feature_importances_
y_pred = gbm1.predict(testdfall[predictors])
y_predprob = gbm1.predict_proba(testdfall[predictors])[:,1]
tr=testdfall[target].values.tolist()
tu=testdfall[IDcol].values.tolist()
# for i in range(0,len(y_predprob)):
#     wfp.write(str(y_predprob[i])+'\t'+str(y_pred[i])+'\t'+str(tr[i])+'\t'+str(tu[i])+'\n')
#     print y_predprob[i],y_pred[i],tr[i],tu[i]
# wfp.close()
print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprob)

feat_imp = pd.Series(gbm1.feature_importances_, predictors).sort_values(ascending=False)

precollist=feat_imp[100:110].index.tolist()
for pre in precollist:
    predictors.remove(pre)
gbm1.fit(traindfall[predictors],traindfall[target])


# print gbm1.feature_importances_
y_pred = gbm1.predict(testdfall[predictors])
y_predprob = gbm1.predict_proba(testdfall[predictors])[:,1]
tr=testdfall[target].values.tolist()
tu=testdfall[IDcol].values.tolist()
# for i in range(0,len(y_predprob)):
#     wfp.write(str(y_predprob[i])+'\t'+str(y_pred[i])+'\t'+str(tr[i])+'\t'+str(tu[i])+'\n')
#     print y_predprob[i],y_pred[i],tr[i],tu[i]
# wfp.close()
print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprob)

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

# feat_imp[0:50].plot(kind='bar', title='Feature Importances')
# plt.ylabel('Feature Importance Score')
# plt.show()




#
# n,bins,patches=plt.hist(y_predprob,100,normed=1,facecolor='g',alpha=0.75)
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
