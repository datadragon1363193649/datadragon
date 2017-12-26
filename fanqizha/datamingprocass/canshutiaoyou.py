import numpy as np
import matplotlib.pyplot as plt

from sklearn import ensemble
from sklearn.model_selection import KFold
from sklearn.model_selection import train_test_split

import pandas as pd
# Generate data (adapted from G. Ridgeway's gbm example)
n_samples = 1000

traindfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
testdfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcoderealrenzheng.csv')
# testdfall=testdfall[testdfall['1']>-1]
# testdfall['1'][(testdfall['1'] > -1) & (testdfall['1'] < 30)] = 0
# testdfall['1'][testdfall['1'] != 0] = 1
# print testdfall['1'].value_counts()
# traindfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcode.csv')
# testdfall=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcoderealrenzheng.csv')
# traindf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/trainnorandcodeandnum.csv')
# testdf=pd.read_csv('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/testnorandcodeandnum.csv')
use_file='/Users/ufenqi/Downloads/18153642368.txt'
linelist=[]
with open(use_file, 'r') as fp:
    for line in fp:
        linelist = line.strip().split(',')
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
# predictors=linelist1
predictors.remove('63')
predictors.remove('73lognor')
predictors.remove('72lognor')
# # predictors.remove('67lognor')
# # predictors.remove('71lognor')
# # predictors.remove('75lognor')
# # predictors.remove('70lognor')
# # predictors.remove('74lognor')
predictors.remove('39lognor')
predictors.remove('39lognorfen')
predictors.remove('40lognor')
predictors.remove('40lognorfen')
# # predictors.remove('41lognor')
# # predictors.remove('41lognorfen')
predictors.remove('42lognor')
predictors.remove('42lognorfen')
X_train=traindfall[predictors]
y_train=traindfall[target]
X_test=testdfall[predictors]
y_test=testdfall[target]

# Fit classifier with out-of-bag estimates
params = {'n_estimators': 1200, 'max_depth': 3, 'subsample': 0.5,
          'learning_rate': 0.01, 'min_samples_leaf': 1, 'random_state': 3}
clf = ensemble.GradientBoostingClassifier(**params)

clf.fit(X_train, y_train)
acc = clf.score(X_test, y_test)
print("Accuracy: {:.4f}".format(acc))

n_estimators = params['n_estimators']
x = np.arange(n_estimators) + 1


def heldout_score(clf, X_test, y_test):
    """compute deviance scores on ``X_test`` and ``y_test``. """
    score = np.zeros((n_estimators,), dtype=np.float64)
    for i, y_pred in enumerate(clf.staged_decision_function(X_test)):
        score[i] = clf.loss_(y_test, y_pred)
    return score


def cv_estimate(n_splits=3):
    cv = KFold(n_splits=n_splits)
    cv_clf = ensemble.GradientBoostingClassifier(**params)
    val_scores = np.zeros((n_estimators,), dtype=np.float64)
    for train, test in cv.split(X_train, y_train):
        cv_clf.fit(X_train[train], y_train[train])
        val_scores += heldout_score(cv_clf, X_train[test], y_train[test])
    val_scores /= n_splits
    return val_scores


# Estimate best n_estimator using cross-validation
cv_score = cv_estimate(3)

# Compute best n_estimator for test data
test_score = heldout_score(clf, X_test, y_test)

# negative cumulative sum of oob improvements
cumsum = -np.cumsum(clf.oob_improvement_)

# min loss according to OOB
oob_best_iter = x[np.argmin(cumsum)]

# min loss according to test (normalize such that first loss is 0)
test_score -= test_score[0]
test_best_iter = x[np.argmin(test_score)]

# min loss according to cv (normalize such that first loss is 0)
cv_score -= cv_score[0]
cv_best_iter = x[np.argmin(cv_score)]

# color brew for the three curves
oob_color = list(map(lambda x: x / 256.0, (190, 174, 212)))
test_color = list(map(lambda x: x / 256.0, (127, 201, 127)))
cv_color = list(map(lambda x: x / 256.0, (253, 192, 134)))

# plot curves and vertical lines for best iterations
plt.plot(x, cumsum, label='OOB loss', color=oob_color)
plt.plot(x, test_score, label='Test loss', color=test_color)
plt.plot(x, cv_score, label='CV loss', color=cv_color)
plt.axvline(x=oob_best_iter, color=oob_color)
plt.axvline(x=test_best_iter, color=test_color)
plt.axvline(x=cv_best_iter, color=cv_color)

# add three vertical lines to xticks
xticks = plt.xticks()
xticks_pos = np.array(xticks[0].tolist() +
                      [oob_best_iter, cv_best_iter, test_best_iter])
xticks_label = np.array(list(map(lambda t: int(t), xticks[0])) +
                        ['OOB', 'CV', 'Test'])
ind = np.argsort(xticks_pos)
xticks_pos = xticks_pos[ind]
xticks_label = xticks_label[ind]
plt.xticks(xticks_pos, xticks_label)

plt.legend(loc='upper right')
plt.ylabel('normalized loss')
plt.xlabel('number of iterations')

plt.show()