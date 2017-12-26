#! /usr/bin/env python
# -*- encoding: utf-8 -*-

import os
BIN_DIR = os.path.dirname(__file__)

# mongodb
mg_host1 = '172.16.51.11:3719'
mg_host2 = '172.16.51.13:3719'
mg_replicat_set = 'test-risk'
mg_uname = 'xuyonglong'
mg_passwd = 'MDkoWEYN3YhBNJpNLyVksdZA'
mg_db = 'risk'
mg_collection = 'call_info'
mg_coll_relation = 'call_relation'
mg_coll_feature = 'user_feature'


# mg_host1 = '172.16.51.11:27017'
# mg_host2 = '172.16.51.11:27017'
# mg_replicat_set = 'test-risk'
# mg_uname = 'risk_user'
# mg_passwd = 'risk_user'
# mg_db = 'risk'
# mg_collection = 'call_info'
# mg_coll_relation = 'call_relation'
# mg_coll_feature = 'base_feature'

# mg_host1 = '172.16.51.11:3717'
# mg_host2 = '172.16.51.13:3717'
# mg_replicat_set = 'test-risk'
# mg_uname = 'xuyonglong'
# mg_passwd = 'MDkoWEYN3YhBNJpNLyVksdZA'
# mg_db = 'risk'
# mg_collection = 'call_info'
# mg_coll_relation = 'call_info'
# mg_coll_feature = 'user_feature'

mg_hostlbs1 = '172.16.51.11:3717'
mg_hostlbs2 = '172.16.51.13:3717'
mg_replicat_set = 'test-risk'
mg_uname = 'xuyonglong'
mg_passwd = 'MDkoWEYN3YhBNJpNLyVksdZA'
mg_collectionlbs='risk_user_lbs'
mg_collection_detail='risk_union_detail'
mg_user_td_detail='risk_user_tongdun_detail'
mg_wecash_taobao_report='wecash_taobao_report'



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
DB_USER_BASIC_INFO = 'mysql://xuyonglong:jfzKssqJFLmUvLMRXpFJxcXEor7qEL@172.16.51.13:65040/warehouse_view?charset=utf8'
# DB_USER_BASIC_INFO_SUDAIBEAR = 'mysql://xuyonglong:MDkoWEYN3YhBNJpNLyVksdZA@172.16.51.13:65000/sudaibear?charset=utf8'
IS_DB_ECHO = False

# model
risk_jk_app_model = os.path.join(BIN_DIR, '../data/model/risk/jk/app')
risk_jk_h5_model = os.path.join(BIN_DIR, '../data/model/risk/jk/h5')

# dict data
risk_contatcs_name = os.path.join(BIN_DIR, '../data/dict/risk/contact_name_cate_word')
fea_schm_file = os.path.join(BIN_DIR, '../data/dict/risk/feature.fs')
bank_operator_list=['10086','10010','10000','95599','95533','95588','95566',
                    '95559','95580','95555','95561','95558','95595','95528',
                    '95568','95501','95528','95501','95568','95508','95577']
tongdun_feature_name=[u'1个月内申请人在多个平台申请借款',u'身份证命中低风险关注名单',
                      u'身份证命中中风险关注名单',u'3个月内身份证关联多个申请信息',
                      u'7天内申请人在多个平台申请借款', u'3个月内申请人在多个平台申请借款',
                      u'6个月内申请人在多个平台申请借款',u'12个月内申请人在多个平台申请借款',
                      u'18个月内申请人在多个平台申请借款',u'24个月内申请人在多个平台申请借款',
                      u'60个月以上申请人在多个平台申请借款', u'手机号命中低风险关注名单',
                      u'申请手机号归属地与真实IP的城市不匹配',u'手机号命中中风险关注名单',
                      u'3个月内申请人手机号作为联系人手机号出现的次数大于等于2',
                      u'1天内身份证使用过多设备进行申请',u'身份证命中信贷逾期名单',
                      u'3个月内申请信息关联多个身份证', u'申请IP与真实IP的城市不匹配',
                      u'7天内身份证使用过多设备进行申请', u'手机号命中高风险关注名单',
                      u'身份证归属地位于高风险较为集中地区']
# tongdun_feature_name=[u'7天内申请人在多个平台申请借款',u'1个月内申请人在多个平台申请借款',
#                       u'3个月内申请人在多个平台申请借款',u'6个月内申请人在多个平台申请借款',
#                       u'12个月内申请人在多个平台申请借款',u'18个月内申请人在多个平台申请借款',
#                       u'24个月内申请人在多个平台申请借款',u'60个月内申请人在多个平台申请借款',
#                       u'手机号命中低风险关注名单',u'手机号命中中风险关注名单',
#                       u'身份证命中低风险关注名单',u'身份证命中中风险关注名单',
#                       u'3个月内身份证关联多个申请信息',u'申请手机号归属地与真实IP的城市不匹配',
#                       u'3个月内申请人手机号作为联系人手机号出现的次数大于等于2',
#                       u'1天内身份证使用过多设备进行申请',u'身份证命中信贷逾期名单',
#                       u'3个月内申请信息关联多个身份证', u'申请IP与真实IP的城市不匹配',
#                       u'7天内身份证使用过多设备进行申请', u'手机号命中高风险关注名单',
#                       u'身份证归属地位于高风险较为集中地区']
operator_dic={u'移动':'10086',u'联通':'10010',u'电信':'10000'}
operator_name_dic={u'移动':0,u'联通':0,u'电信':0,u'话费':0}
member_type_dic={u'V1会员':1,u'V2会员':2,u'V3会员':3,u'V4会员':4,u'V5会员':5}
