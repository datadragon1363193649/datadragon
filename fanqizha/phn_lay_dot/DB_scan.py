# -*- cocoding: utf-8 -*-
import matplotlib.pylab as plt
import pandas as pd
path = "./../../data/crawl/dangerous_area/GPS/jinan.info"
data = pd.read_csv(path,sep="\t")
data.head()
# plt.scatter(data.loc[:,"lng"],data.loc[:,"lat"],linewidths=0.5)
# plt.xticks(np.linspace(116,118,30))
# plt.yticks(np.linspace(36,38,20))
# # plt.figure(figsize=(600,100))
# plt.gca().xaxis.set_major_formatter(ticker.FormatStrFormatter('%.3f'))
# plt.show()

from sklearn.cluster import DBSCAN
X = data[["lng","lat"]].values

y_pred = DBSCAN(eps = 0.001, min_samples = 2).fit_predict(X)
plt.scatter(X[:, 0], X[:, 1], c=y_pred)
plt.show()
import lime
