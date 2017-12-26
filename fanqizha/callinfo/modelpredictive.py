# -*- cocoding: utf-8 -*-
import os
import sys

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath = os.path.split(os.path.realpath(_abs_path))[0]
sys.path.append(apath)
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
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import seaborn as sns
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import roc_curve
from sklearn.externals import joblib
# lr是一个LogisticRegression模型
# joblib.dump(lr, 'lr.model')
# lr = joblib.load('lr.model')
import config.offline_db_conf as dconf
import pymongo as pm
import logging
joblib.load('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/model/call.model')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=apath + '/log/' + dconf.log_filename,
                    filemode='a')
_debug = False


class Modelpredictive(object):
    def __init__(self):
        self.mhost1 = dconf.mg_host1
        self.mhost2 = dconf.mg_host2
        self.mreplicat_set = dconf.mg_replicat_set
        self.mgdb = dconf.mg_db
        self.mgcollection = dconf.mg_collection
        self.mgcollectionrelation = dconf.mg_coll_relation
        self.muname = dconf.mg_uname
        self.mpasswd = dconf.mg_passwd
        self.mgconn = None
        self.init_mg_conn()
        self.mysqlhost = dconf.mysql_host
        self.mysqlport = dconf.mysql_port
        self.mysqluser = dconf.mysql_user
        self.mysqlpasswd = dconf.mysql_passwd
        self.mysqldb = dconf.mysql_db
        self.mysqlconn = None
        self.init_mysql_conn()
        self.featurename=[]
        self.get_featurename()
        self.featurevalue=[]
        self.featurevaluelist = []
        self.lr=None

    # def init_mysql_conn(self):
    #     self.mysqlconn= MySQLdb.connect(host=self.mysqlhost, port=self.mysqlport, user=self.mysqluser, passwd=self.mysqlpasswd,
    #                           db=self.mysqldb, charset="utf8")
    def lode_model(self):
        self.lr=joblib.load('/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/model/call.model')
    def init_mg_conn(self):
        self.mgconn = pm.MongoClient([self.mhost1, self.mhost2], replicaSet=self.mreplicat_set, maxPoolSize=10)
        self.mgconn[dconf.mg_db].authenticate(self.muname, self.mpasswd)

    def get_mg_conn(self):
        mc = self.mgcollection
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]

    def get_mg_connrelation(self):
        mc = self.mgcollectionrelation
        mdb = self.mgconn[self.mgdb]
        return mdb[mc]
    def get_featurename(self):

           aaa=1

    def get_feature(self,phn):

        cf = self.get_mg_conn(dconf.mg_coll_fea)
        rs = cf.find({'_id': phn})
        if not rs or rs.count() == 0:
            # print rs
            return 0
        if 'call' in rs:
            for fea in self.featurename:
                if fea in rs['call']:
                    self.featurevalue.append(rs['call'][fea])
                else:
                    self.featurevalue.append(-1)
        else:
            return 0
    # 数据预处理
    def get_ceiling(self):
        aaa=1
    # log处理和归一化处理
    def get_loghandle(self):
        aaa=1
    # 分箱
    def get_binning(self):
        aaa=1
    def fea_preprocessing(self):
        self.get_ceiling()
        self.get_loghandle()
        self.get_binning()
    def predictive_one(self):
        self.lr.predict_proba(self.featurevalue)[:,1]

    def predictive(self):
        bb = 1
if __name__ == '__main__':
    m=Modelpredictive()
    phn='1'
    m.get_feature(phn)


