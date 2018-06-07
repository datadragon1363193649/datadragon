#! /usr/bin/env python
# -*- encoding: utf-8 -*-

# mongodb
mg_hosty1 = '172.16.51.11:27017'
mg_hosty2 = '172.16.51.13:27017'
mg_dby = 'risk'
mg_collectiony = 'call_info'
mg_coll_relationy = 'call_relation'
mg_coll_fea='base_feature'



mg_host1 = '172.16.51.11:27017'
mg_host2 = '172.16.51.13:27017'
mg_replicat_set = 'test-risk'
mg_uname = 'risk_user'
mg_passwd = 'risk_user'
mg_db = 'risk'
mg_collection = 'call_info'
mg_coll_relation = 'call_relation'
# mysql
mysql_host='172.16.51.13'
mysql_port=65000
mysql_user='xuyonglong'
mysql_passwd='MDkoWEYN3YhBNJpNLyVksdZA'
mysql_db='risk'
log_filename='callinfo.log'


mysql_host_test='172.16.51.14'
mysql_port_test=3306
mysql_user_test='test'
mysql_passwd_test='test'
mysql_db_fea='base_feature'
data_path='/Users/ufenqi/Documents/dataming/phn_risk/dataset/tongdun/'
config_path='/Users/ufenqi/Documents/dataming/phn_risk/parameter/tongdun/'
model_path='/Users/ufenqi/Documents/dataming/phn_risk/model/'
featurename='featurename_online_use_tongdun'
class_feature_name=['110','111','454','455','456','461','464']