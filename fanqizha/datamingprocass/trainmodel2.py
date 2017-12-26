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

traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindata_code2.csv')
testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdata_code2.csv')

traindfnor=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfnor.csv')
testdfnor=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testdfnor.csv')

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

predictors = [x for x in traindf.columns if x not in [target, IDcol] ]
# predictors.remove('39lognorfen')
# predictors.remove('40lognorfen')
# predictors.remove('41lognor')
# predictors.remove('41lognorfen')
predictors.remove('42lognorfen')
# predictors.remove('73lognor')
# predictors.remove('72lognor')
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


gbm1.fit(traindf[predictors],traindf[target])


# print gbm1.feature_importances_
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

feat_imp = pd.Series(gbm1.feature_importances_, predictors).sort_values(ascending=False)
precollist=feat_imp[0:70].index.tolist()


predictors = [x for x in traindfnor.columns if x not in [target, IDcol] ]
predictors.remove('73lognor')
predictors.remove('72lognor')




gbm1.fit(traindfnor[predictors],traindfnor[target])


# print gbm1.feature_importances_
y_pred = gbm1.predict(testdfnor[predictors])
y_predprob = gbm1.predict_proba(testdfnor[predictors])[:,1]
tr=testdfnor[target].values.tolist()
tu=testdfnor[IDcol].values.tolist()

print "Accuracy : %.4g" % metrics.accuracy_score(testdfnor[target], y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfnor[target], y_predprob)

feat_imp = pd.Series(gbm1.feature_importances_, predictors).sort_values(ascending=False)
precollistnor=feat_imp[0:60].index.tolist()
precollist+=precollistnor
precollist=list(set(precollist))
print precollist
print len(precollist)
traindfall1=traindfall[precollist]

traindfall1.to_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/traindfall.csv',index=False)


gbm1.fit(traindfall[predictors],traindfall[target])

y_pred = gbm1.predict(testdfall[predictors])
y_predprob = gbm1.predict_proba(testdfall[predictors])[:,1]
tr=testdfall[target].values.tolist()
tu=testdfall[IDcol].values.tolist()
print "Accuracy : %.4g" % metrics.accuracy_score(testdfall[target], y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(testdfall[target], y_predprob)




