# -*- cocoding: utf-8 -*-
import numpy as np
import math
from scipy import stats
from sklearn.utils.multiclass import type_of_target
import pandas as pd
import sys
import offline_db_conf as dconf
import json
reload(sys)

sys.setdefaultencoding('utf-8')

class WOE:
    def __init__(self):
        self._WOE_MIN = -20
        self._WOE_MAX = 20

    def woe(self, X, y, event=1):
        '''
        Calculate woe of each feature category and information value
        :param X: 2-D numpy array explanatory features which should be discreted already
        :param y: 1-D numpy array target variable which should be binary
        :param event: value of binary stands for the event to predict
        :return: numpy array of woe dictionaries, each dictionary contains woe values for categories of each feature
                 numpy array of information value of each feature
        '''
        self.check_target_binary(y)
        X1 = self.feature_discretion(X)

        res_woe = []
        res_iv = []
        for i in range(0, X1.shape[-1]):
            x = X1[:, i]
            woe_dict, iv1 = self.woe_single_x(x, y, event)
            res_woe.append(woe_dict)
            res_iv.append(iv1)
        return np.array(res_woe), np.array(res_iv)

    def woe_single_x(self, x, y, event=1):
        '''
        calculate woe and information for a single feature
        :param x: 1-D numpy starnds for single feature
        :param y: 1-D numpy array target variable
        :param event: value of binary stands for the event to predict
        :return: dictionary contains woe values for categories of this feature
                 information value of this feature
        '''
        self.check_target_binary(y)

        event_total, non_event_total = self.count_binary(y, event=event)
        x_labels = np.unique(x)
        woe_dict = {}
        iv = 0
        for x1 in x_labels:
            y1 = y[np.where(x == x1)[0]]
            event_count, non_event_count = self.count_binary(y1, event=event)
            rate_event = 1.0 * event_count / event_total
            rate_non_event = 1.0 * non_event_count / non_event_total
            if rate_event == 0:
                woe1 = self._WOE_MIN
            elif rate_non_event == 0:
                woe1 = self._WOE_MAX
            else:
                woe1 = math.log(rate_event / rate_non_event)
            woe_dict[x1] = woe1
            iv += (rate_event - rate_non_event) * woe1
        return woe_dict, iv

    def woe_replace(self, X, woe_arr):
        '''
        replace the explanatory feature categories with its woe value
        :param X: 2-D numpy array explanatory features which should be discreted already
        :param woe_arr: numpy array of woe dictionaries, each dictionary contains woe values for categories of each feature
        :return: the new numpy array in which woe values filled
        '''
        if X.shape[-1] != woe_arr.shape[-1]:
            raise ValueError('WOE dict array length must be equal with features length')

        res = np.copy(X).astype(float)
        idx = 0
        for woe_dict in woe_arr:
            for k in woe_dict.keys():
                woe = woe_dict[k]
                res[:, idx][np.where(res[:, idx] == k)[0]] = woe * 1.0
            idx += 1

        return res

    def combined_iv(self, X, y, masks, event=1):
        '''
        calcute the information vlaue of combination features
        :param X: 2-D numpy array explanatory features which should be discreted already
        :param y: 1-D numpy array target variable
        :param masks: 1-D numpy array of masks stands for which features are included in combination,
                      e.g. np.array([0,0,1,1,1,0,0,0,0,0,1]), the length should be same as features length
        :param event: value of binary stands for the event to predict
        :return: woe dictionary and information value of combined features
        '''
        if masks.shape[-1] != X.shape[-1]:
            raise ValueError('Masks array length must be equal with features length')

        x = X[:, np.where(masks == 1)[0]]
        tmp = []
        for i in range(x.shape[0]):
            tmp.append(self.combine(x[i, :]))

        dumy = np.array(tmp)
        # dumy_labels = np.unique(dumy)
        woe, iv = self.woe_single_x(dumy, y, event)
        return woe, iv

    def combine(self, list):
        res = ''
        for item in list:
            res += str(item)
        return res

    def count_binary(self, a, event=1):
        event_count = (a == event).sum()
        non_event_count = a.shape[-1] - event_count
        return event_count, non_event_count

    def check_target_binary(self, y):
        '''
        check if the target variable is binary, raise error if not.
        :param y:
        :return:
        '''
        y_type = type_of_target(y)
        if y_type not in ['binary']:
            raise ValueError('Label type must be binary')

    def feature_discretion(self, X):
        '''
        Discrete the continuous features of input data X, and keep other features unchanged.
        :param X : numpy array
        :return: the numpy array in which all continuous features are discreted
        '''
        temp = []
        for i in range(0, X.shape[-1]):
            x = X[:, i]
            x_type = type_of_target(x)
            if x_type == 'continuous':
                x1 = self.discrete(x)
                temp.append(x1)
            else:
                temp.append(x)
        return np.array(temp).T

    def discrete(self, x):
        '''
        Discrete the input 1-D numpy array using 5 equal percentiles
        :param x: 1-D numpy array
        :return: discreted 1-D numpy array
        '''
        res = np.array([0] * x.shape[-1], dtype=int)
        for i in range(5):
            point1 = stats.scoreatpercentile(x, i * 20)
            point2 = stats.scoreatpercentile(x, (i + 1) * 20)
            x1 = x[np.where((x >= point1) & (x <= point2))]
            mask = np.in1d(x, x1)
            res[mask] = (i + 1)
        return res

    @property
    def WOE_MIN(self):
        return self._WOE_MIN
    @WOE_MIN.setter
    def WOE_MIN(self, woe_min):
        self._WOE_MIN = woe_min
    @property
    def WOE_MAX(self):
        return self._WOE_MAX
    @WOE_MAX.setter
    def WOE_MAX(self, woe_max):
        self._WOE_MAX = woe_max
    def train(self,dfname):
        f_file = dconf.data_path + dfname
        out_name = 'woe' + dfname
        out_file = dconf.data_path + out_name
        json_file = dconf.config_path + 'woe_value.json'
        s_file = dconf.config_path + dconf.featurename
        wfp=open(json_file,'w')
        feanamedic={}
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feanamedic[linelist[0]] = linelist[1]
        traindf = pd.read_csv(f_file)
        target = '1'
        IDcol = '0'
        predictors = [x for x in traindf.columns if x not in [target, IDcol]]
        tn = traindf[predictors].values
        b, c = self.woe(tn, traindf[target])
        # print b
        if len(predictors)!=len(b):
            raise ValueError('Masks array length must be equal with features length')
        woedic={}
        print traindf.dtypes
        for i in range(0,len(predictors)):
            woe_list=b[i]
            woe_keylist=woe_list.keys()
            woe_valuelist=woe_list.values()
            # print woe_keylist
            # print woe_valuelist
            traindf[predictors[i]].replace(woe_keylist, woe_valuelist, inplace=True)
            woedic[feanamedic[predictors[i]]]=b[i]
        json.dump(woedic, wfp)
        for col in predictors:
            traindf[col] = traindf[col].astype('float64')
        # print traindf.dtypes
        traindf.to_csv(out_file, index=False)
        # trainnp = a.woe_replace(tn, b)
    def test(self,dfname):
        f_file = dconf.data_path + dfname
        out_name = 'woe' + dfname
        out_file = dconf.data_path + out_name
        json_file = dconf.config_path + 'woe_value.json'
        s_file = dconf.config_path + dconf.featurename
        testdf = pd.read_csv(f_file)
        feaidlist = []
        feanamedic = {}
        # feaidlist.append('0')
        if '1' in testdf.columns.tolist():
            flist = ['0', '1']
        else:
            flist = ['0']
        with open(s_file, 'r') as fp:
            for line in fp:
                linelist = line.strip().split(',')
                feaidlist.append(linelist[0])
                flist.append(linelist[0])
                feanamedic[linelist[0]] = linelist[1]
        testdf = testdf[flist]
        columns = testdf.columns.tolist()
        columns.remove('0')
        # print columns
        if '1' in columns:
            columns.remove('1')
        with open(json_file, 'r') as fp:
            c = fp.readline()
            c = json.loads(c)
        for col in columns:
            woe_list = c[feanamedic[col]]
            woe_keylist_str = woe_list.keys()
            woe_valuelist = woe_list.values()
            woe_keylist=[]
            for ks in woe_keylist_str:
                woe_keylist.append(float(ks))
            # print col
            print woe_keylist
            print woe_valuelist
            # print testdf.dtypes
            testdf[col].replace(woe_keylist, woe_valuelist, inplace=True)
        for col in columns:
            testdf[col] = testdf[col].astype('float64')
        print testdf.dtypes
        testdf.to_csv(out_file, index=False)

if __name__ == '__main__':
    a=WOE()
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        a.train(input_file)
    if tat == 'test':
        a.test(input_file)
    # traindf = pd.read_csv('/Users/ufenqi/Downloads/trainnoonehot1.csv')
    # testdf = pd.read_csv('/Users/ufenqi/Downloads/testnoonehot1.csv')
    # target = '1'
    # IDcol = '0'
    # predictors = [x for x in traindf.columns if x not in [target, IDcol]]
    # print traindf.head()
    # tn=traindf[predictors].values
    # b,c=a.woe(tn,traindf[target])
    # trainnp=a.woe_replace(tn,b)
    # testn=testdf[predictors]
    # trainnp = a.woe_replace(tn, b)
    # testnp = a.woe_replace(testn, b)
    # # print d[0]
    # train1df=pd.DataFrame(trainnp,columns=predictors)
    # test1df=pd.DataFrame(testnp,columns=predictors)
    # train1df['0']=traindf['0']
    # train1df['1'] = traindf['1']
    # test1df['0'] = testdf['0']
    # test1df['1'] = testdf['1']
    # test1df.to_csv('/Users/ufenqi/Downloads/testwoe1.csv', index=False)
    # train1df.to_csv('/Users/ufenqi/Downloads/trainwoe1.csv', index=False)
    # print b
    # print d[1]
    # print tn[1]