#! /usr/bin/env python
# -*- encoding: utf-8 -*-

# mongodb
mg_host1 = '172.16.51.11:27017'
mg_host2 = '172.16.51.13:27017'
mg_replicat_set = 'test-risk'
mg_uname = 'risk_user'
mg_passwd = 'risk_user'
mg_db = 'risk'
mg_collection = 'call_info'
mg_coll_relation = 'call_relation'

# mysql_credit
mysql_host_credit='172.16.51.13'
mysql_port_credit=65000
mysql_user_credit='xuyonglong'
mysql_passwd_credit='MDkoWEYN3YhBNJpNLyVksdZA'
mysql_db_credit='risk'

# mysql_collect
mysql_host_collect='172.16.51.13'
mysql_port_collect=65000
mysql_user_collect='xuyonglong'
mysql_passwd_collect='MDkoWEYN3YhBNJpNLyVksdZA'
mysql_db_collect='risk'

log_filename_collect='callinfo.log'

# log
log_path='/log/'