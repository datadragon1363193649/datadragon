# -*- cocoding: utf-8 -*-
import sys
import offline_db_conf as dconf
from hetroneModel.subfunction import *
from hetroneModel.feature.FeatureEngineering import *
from hetroneModel.model.BuildModel import *

reload(sys)
sys.setdefaultencoding('utf-8')
'''
结合数据处理框架
'''

def train_fea():
    dirname_model = dconf.config_path
    sample_file='D:/xuyl/data/integration_xyl/data/train_uid_sample0'
    samplelist=[]
    with open(sample_file,'r') as fp:
        for line in fp:
            samplelist.append(line.strip())
    tdf=pd.read_csv(dconf.data_path+'delindclasstrain_uid_target_all_tongdun1.csv')
    outweight_file = dconf.config_path + 'tongdun_weight.json'
    model_file=dconf.model_path+'tongdun_model'
    tdf=tdf.set_index(['0'])

    s_file = dconf.config_path + dconf.featurename
    jsonc_file = dconf.config_path + 'classfeaname.json'
    feaidlist = []
    feanamelist=[]
    feanamedic = {}
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
            feanamedic[linelist[0]] = linelist[1]
    xdf=tdf[feaidlist]
    xdf.columns=feanamelist
    print xdf.columns
    ydf=tdf['1']
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    classlist=[]
    numberlist=[]
    for feid in feanamedic:
        if feanamedic[feid] in cc:
            classlist.append(feanamedic[feid])
        elif feid in dconf.class_feature_name:
            classlist.append(feanamedic[feid])
        else:
            numberlist.append(feanamedic[feid])
    print classlist
    fields_dict={'category':classlist,'numerical':numberlist}
    tfi = TrainFeatureClass(dirname_model)
    # x_train = tfi.apply_train(X,Y)

    # 设定特征格式
    dtypes = tfi.get_dtypes(fields_dict)
    df_dtypes = tfi.set_dtypes(xdf,dtypes)
    fields_num = fields_dict['numerical']
    ## 对值做log处理
    min_max_dict = tfi.get_min_max(xdf.copy(), fields_num)
    print min_max_dict
    joblib.dump(min_max_dict,dconf.config_path+'log_value')
    df_log = tfi.set_log(xdf.copy(), min_max_dict)
    # print df_log.head()
    # 归一化
    min_max_dict = tfi.get_min_max(df_log.copy(), fields_num)
    joblib.dump(min_max_dict, dconf.config_path+'norin_value')
    # print 'store min_max_dict sucessfully in %s !' % self.dirname_model
    df_min_max = tfi.set_normal(df_log.copy(), min_max_dict)
    # print df_min_max.head()
    ## 分箱处理
    dict_bins_train = tfi.get_bins(df_min_max.copy(), numberlist)
    joblib.dump(dict_bins_train, dconf.config_path + '/binning_value')
    # print 'store dict_bins_train sucessfully in %s !' % tfi.dirname_model
    df_bins = tfi.set_bins(df_min_max.copy(), dict_bins_train)
    df_bins.to_csv(dconf.data_path+'binningdf.csv')
    nodf=df_bins[df_bins.index.isin(samplelist)]

    df_bins=df_bins.drop(nodf.index)
    ynodf = ydf[ydf.index.isin(samplelist)]
    ydf = ydf.drop(ynodf.index)
    print len(ynodf)
    print len(ydf)
    # 建模
    logm = LogisticRegression(C=1.0,penalty='l1',solver='liblinear',multi_class='ovr')
    logm.fit(df_bins,ydf)
    wfp = open(outweight_file, 'w')
    weightlist = logm.coef_[0]
    # # print weightlist
    featureweidc = {}
    # print nordic
    for i in range(0, len(feanamelist)):
        print feanamelist[i], weightlist[i]
        featureweidc[feanamelist[i]] = round(weightlist[i], 10)
    featureweidc['bias'] = round(logm.intercept_[0], 10)
    json.dump(featureweidc, wfp)
    joblib.dump(logm, model_file)
    # feat_imp = pd.Series(logm.coef_[0], feanamelist).sort_values(ascending=False)
    # # print feat_imp
    # feat_imp.plot(kind='bar', title='Feature Importances')
    # plt.ylabel('Feature Importance Score')
    # plt.show()
def test_fea():
    dirname_model = dconf.config_path
    tdf = pd.read_csv(dconf.data_path + 'delclasstest_data_90101.csv')
    outfile=dconf.data_path+'score_tongdun_9010'
    # outweight_file = dconf.config_path + 'tongdun_weight.json'
    model_file = dconf.model_path + 'tongdun_model'
    tdf = tdf.set_index(['0'])
    s_file = dconf.config_path + dconf.featurename
    jsonc_file = dconf.config_path + 'classfeaname.json'
    feaidlist = []
    feanamelist = []
    feanamedic = {}
    with open(s_file, 'r') as fp:
        for line in fp:
            linelist = line.strip().split(',')
            feaidlist.append(linelist[0])
            feanamelist.append(linelist[1])
            feanamedic[linelist[0]] = linelist[1]
    xdf = tdf[feaidlist]
    xdf.columns = feanamelist
    print xdf.columns
    # ydf = tdf['1']
    with open(jsonc_file, 'r') as fp:
        cc = fp.readline()
        cc = json.loads(cc)
    classlist = []
    numberlist = []
    for feid in feanamedic:
        if feanamedic[feid] in cc:
            classlist.append(feanamedic[feid])
        elif feid in dconf.class_feature_name:
            classlist.append(feanamedic[feid])
        else:
            numberlist.append(feanamedic[feid])
    print classlist
    fields_dict = {'category': classlist, 'numerical': numberlist}
    tfi = TrainFeatureClass(dirname_model, fields_dict)
    ## 对值做log处理
    min_max_dict = joblib.load(dconf.config_path + 'log_value')
    df_log = tfi.set_log(xdf.copy(), min_max_dict)

    ## 归一化处理
    min_max_dict = joblib.load(dconf.config_path + 'norin_value')
    df_min_max = tfi.set_normal(df_log.copy(), min_max_dict)

    ## 分箱处理
    dict_bins_train = joblib.load(dconf.config_path + 'binning_value')
    df_bins = tfi.set_bins(df_min_max.copy(), dict_bins_train)
    print df_bins.head()
    logm = joblib.load(model_file)
    y_predl = logm.predict(df_bins)
    y_predprobl = logm.predict_proba(df_bins)[:, 1]

    wfp = open(outfile, 'w')
    # # # print y_predprob
    userid = df_bins.index.values.tolist()
    # usery=testdfall['1'].values.tolist()
    # # #

    for ui in range(0, len(userid)):
        wfp.write(str(userid[ui]) + ',' + str(y_predprobl[ui]) + '\n')

if __name__ == '__main__':
    train_fea()
    # test_fea()

