#! /usr/bin/env python
# -*- encoding: utf-8 -*-

import os
BIN_DIR = os.path.dirname(__file__)

# mongodb
mg_host1 = '172.16.51.11:3717'
mg_host2 = '172.16.51.13:3717'
mg_replicat_set = 'test-risk'
mg_uname = 'xuyonglong'
mg_passwd = 'MDkoWEYN3YhBNJpNLyVksdZA'
mg_db = 'risk'
mg_collection = 'call_info'
mg_coll_relation = 'call_relation'
mg_coll_feature = 'user_feature'

mg_coll_tonfdun='risk_user_tongdun'
mg_coll_tonfdun_detail='risk_user_tongdun_detail'
mg_coll_user_tonfdun='cc_user_tongdun'
mg_coll_user_qunar='risk_user_qunar'

mg_collectionlbs='risk_user_lbs'
mg_collection_detail='risk_union_detail'


mg_hosty1 = '172.16.51.11:27017'
mg_hosty2 = '172.16.51.13:27017'
mg_dby = 'risk'
mg_collectiony = 'call_info'
mg_coll_relationy = 'call_relation'
mg_coll_fea='base_feature'

# mongodb old
o_mg_host1 = 'mongo1.test.in'
o_mg_host2 = 'mongo2.test.in'
o_mg_replicat_set = 'test-risk'
o_mg_uname = 'risk_user'
o_mg_passwd = 'risk_user'
o_mg_db = 'risk'
o_mg_collection = 'call_info'

# mysql
DB_USER_BASIC_INFO = 'mysql://xuyonglong:MDkoWEYN3YhBNJpNLyVksdZA@172.16.51.13:65000/risk?charset=utf8'
DB_USER_BASIC_INFO_SUDAIBEAR = 'mysql://xuyonglong:MDkoWEYN3YhBNJpNLyVksdZA@172.16.51.13:65000/sudaibear?charset=utf8'
IS_DB_ECHO = False

# model
risk_jk_app_model = os.path.join(BIN_DIR, '../data/model/risk/jk/app')
risk_jk_h5_model = os.path.join(BIN_DIR, '../data/model/risk/jk/h5')

# dict data
risk_contatcs_name = os.path.join(BIN_DIR, '../data/dict/risk/contact_name_cate_word')
fea_schm_file = os.path.join(BIN_DIR, '../data/dict/risk/feature.fs')




