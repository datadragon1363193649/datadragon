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
train=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfdel.csv')
refusedf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata02.csv')
reuserid=refusedf['0'].values.tolist()

traindf=train.sample(frac=0.75,random_state=22)
testdf=train.drop(traindf.index)
testuserid=testdf['0'].values.tolist()
print testdf.__len__()
relist=[]
# for re in reuserid:
#     if re in testuserid:
#         relist.append(re)
#         testdf=testdf.drop(train[train['0']==re].index, axis=0)
print testdf.__len__()
print len(relist)
target='1'
IDcol='0'
# print testdf[target].value_counts()

predictors = [x for x in traindf.columns if x not in [target, IDcol] ]
print predictors
# predictors.remove('n0')
# predictors.remove('n1')
# predictors.remove('n2')
# predictors.remove('n3')
# predictors.remove('n4')
# predictors.remove('n5')
# predictors.remove('n6')
testpredictors=[]
for pre in predictors:
    testpredictors.append(pre.split('_')[0])



# gbm1 = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60,max_depth=7, min_samples_leaf =100,
#                min_samples_split =1200, max_features='sqrt', random_state=10)
#
# gbm1.fit(traindf[predictors],traindf[target])
# print gsearch1.grid_scores_, gsearch1.best_params_, gsearch1.best_score_










# grd_lm = LogisticRegression()





# gbm1 = GradientBoostingClassifier(learning_rate=0.1, n_estimators=120,max_depth=7, min_samples_leaf =70,
#                min_samples_split =800, max_features=17, random_state=10)
gbm1 = GradientBoostingClassifier()




# out_file='/Users/ufenqi/Downloads/jieguo1'
# wfp = open(out_file, 'w')
gbm1.fit(traindf[predictors],traindf[target])


print gbm1.feature_importances_
y_pred = gbm1.predict(testdf[predictors])
y_predprob = gbm1.predict_proba(testdf[predictors])[:,1]
tr=testdf[target].values.tolist()
tu=testdf[IDcol].values.tolist()
# for i in range(0,len(y_predprob)):
#     wfp.write(str(y_predprob[i])+'\t'+str(y_pred[i])+'\t'+str(tr[i])+'\t'+str(tu[i])+'\n')
#     print y_predprob[i],y_pred[i],tr[i],tu[i]
# wfp.close()
print "Accuracy : %.4g" % metrics.accuracy_score(testdf[target], y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(testdf[target], y_predprob)
#
#
#
#
#
#
# feat_imp = pd.Series(gbm1.feature_importances_, predictors).sort_values(ascending=False)
# feat_imp.plot(kind='bar', title='Feature Importances')
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
