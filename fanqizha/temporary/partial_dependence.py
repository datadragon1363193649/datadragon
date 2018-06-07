# -*- cocoding: utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt

from mpl_toolkits.mplot3d import Axes3D

from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble.partial_dependence import plot_partial_dependence
from sklearn.ensemble.partial_dependence import partial_dependence
import seaborn as sns
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#可以加载加州房价数据集
from sklearn.datasets.california_housing import fetch_california_housing

from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei'] #指定默认字体
mpl.rcParams['axes.unicode_minus'] = False #解决保存图像是负号'-'显示为方
#先获取数据集
cal_housing = fetch_california_housing()

# 拆分训练测试集
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                    cal_housing.target,
                                                    test_size=0.2,
                                                    random_state=1)
names = cal_housing.feature_names
print names
print X_train['MedInc']
# clf = GradientBoostingRegressor(n_estimators=100, max_depth=4,
#                                 learning_rate=0.1, loss='huber',
#                                 random_state=1)
# clf.fit(X_train, y_train)
plt.scatter(X_train['MedInc'].tolist(),y_train.tolist() )
plt.show()
#
# plot_partial_dependence(clf, X_train,
#      ['MedInc'],#需要绘制的X变量
#     feature_names=names, #变量列表
#     n_jobs=3, grid_resolution=50)
# plt.title('偏依赖图')
# plt.ylabel('房价')
# plt.show()