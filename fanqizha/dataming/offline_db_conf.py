#! /usr/bin/env python
# -*- encoding: utf-8 -*-

# mongodb
# mg_hosty1 = '172.16.51.11:3719'
# mg_hosty2 = '172.16.51.13:3719'
# mg_dby = 'risk'
# mg_collectiony = 'call_info'
# mg_coll_relationy = 'call_relation'
# mg_coll_fea='user_feature'
# mg_uname = 'xuyonglong'
# mg_passwd = 'MDkoWEYN3YhBNJpNLyVksdZA'




# mg_hosty1 = '172.16.51.11:3719'
# mg_hosty2 = '172.16.51.13:3720'
# mg_replicat_set = 'test-risk'
# mg_uname = 'xuyonglong'
# mg_passwd = 'MDkoWEYN3YhBNJpNLyVksdZA'
# mg_dby = 'risk'
# mg_collection = 'call_info'
# mg_coll_relation = 'call_relation'
# mg_coll_feature = 'user_feature'

#
mg_hosty1 = '172.16.51.11:27017'
mg_hosty2 = '172.16.51.11:27017'
mg_replicat_set = 'test-risk'
mg_uname = 'risk_user'
mg_passwd = 'risk_user'
mg_dby = 'risk'
mg_collection = 'call_info'
mg_coll_relation = 'call_relation'
mg_coll_feature = 'base_feature'


# mysql
mysql_host='172.16.51.13'
mysql_port=65000
mysql_user='xuyonglong'
mysql_passwd='MDkoWEYN3YhBNJpNLyVksdZA'
mysql_db='risk'
mysql_dbf='base_feature'
log_filename='callinfo.log'


# data_path='/Users/ufenqi/Documents/dataming/base1/data/data_1510/test/'
# data_path='/Users/ufenqi/Documents/dataming/base1/test/'
# data_path='/Users/ufenqi/Documents/dataming/base2/overdue/'
# data_path='/Users/ufenqi/Documents/dataming/model_yu/data/'
# data_path='/Users/ufenqi/Downloads/fanqizha/2017fanqizhamax/'
# config_path='/Users/ufenqi/Documents/dataming/base1/config/data_1510/bank/test2/'
# model_path='/Users/ufenqi/Documents/dataming/base1/model/data_1510/bank/test2/'
# config_path='/Users/ufenqi/Documents/dataming/model_yu/config/m1_test/'

# config_path='/Users/ufenqi/Documents/dataming/base1/config/data_1510/bank/bank_conf/'
# model_path='/Users/ufenqi/Documents/dataming/model_yu/model/'
# model_path='/Users/ufenqi/Documents/dataming/base1/model/data_1510/bank/'

# data_path='/Users/ufenqi/Documents/dataming/taobao/data/'
# config_path='/Users/ufenqi/Documents/dataming/taobao/config/t1/t1_2/'
# model_path='/Users/ufenqi/Documents/dataming/taobao/model/'
# 文件路径
data_path='/Users/ufenqi/Documents/dataming/phn_risk/phn_lay_dot/dataset/'
config_path='/Users/ufenqi/Documents/dataming/phn_risk/parameter/tongdun/'
model_path='/Users/ufenqi/Documents/dataming/phn_risk/model/'
# 列名文件名称
# featurename='featurename_online_use'
# featurename='featurename_onlinebank_column_all'
# featurename='featurename_onlinetest_call'
# featurename='featurename_online_call'
# featurename='featurename_onlinetest_call_old'
# featurename='featurename_onlinetest_new_detail'
# featurename='featurename_onlinetest_new_tongdun'
# featurename='featurename_onlinetest_call'
# featurename='featurename_onlinetest_all_first'
# featurename='featurename_onlinetest_new'
# featurename='featurename_online_use_call'
# featurename='featurename_online_use_app'
featurename='featurename_online_use_tongdun'
# featurename='featurename_online_use_bank'
# featurename='featurename_online_use_score_card'
# featurename='featurename_online_use_integration'
# featurename='featurename_online_use_score_card'
# featurename='featurename_online'
# 类别型名字
# bank
# class_feature_name=['159','158','134','188','408','162','412']
# tongdun
class_feature_name=['110','111','454','455','456','461','464']
# app
# class_feature_name=['63','2','465','467','471','472','473','474','475']
# class_feature_name = ['63','2']
# class_feature_name=['bj_zj','bj_zj1','bj_zj2']
# class_feature_name=[]
# 运营商电话
operator_dic={u'移动':'10086',u'联通':'10010',u'电信':'10000'}
# 用户ID
uid='0'
# 目标值
target='1'