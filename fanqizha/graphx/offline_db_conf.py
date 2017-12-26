#! /usr/bin/env python
# -*- encoding: utf-8 -*-

# mongodb
mg_host1 = '172.16.51.13:3719'
mg_host2 = '172.16.51.13:3719'
mg_replicat_set = 'test-risk'
mg_uname = 'xuyonglong'
mg_passwd = 'MDkoWEYN3YhBNJpNLyVksdZA'
mg_db = 'risk'
mg_collection = 'call_info'
mg_coll_relation = 'call_relation'
# mysql
# mysql_host='172.16.51.13'
# mysql_port=65000
# mysql_user='xuyonglong'
# mysql_passwd='MDkoWEYN3YhBNJpNLyVksdZA'
# mysql_db='risk'
# log_filename='callinfo.log'

mysql_host='172.16.51.13'
mysql_port=65040
mysql_user='xuyonglong'
mysql_passwd='HnhpdgvC7Coef3MP'
mysql_db='warehouse'
mysql_dbf='base_feature'
log_filename='modelinfo.log'
daylist=['2017-04-17','2017-04-18','2017-04-19','2017-04-20',
         '2017-04-21','2017-04-22','2017-04-23','2017-04-24','2017-04-25',
         '2017-04-26', '2017-04-27', '2017-04-28','2017-04-29','2017-04-30','2017-05-01']
oneday='2017-05-01'
prephnlist=['13027182678','13068602206','13101625333','13107933396','13126125979','13171392949','13218651199','13246777538',
             '13263096702','13264860018','13886708480','13978096787','15890753685','18678668355','18734832489','18765268687',
             '18959091441','13450723983','13629685693','13718893389','18688068675','13810437118','17792219729','13794032225',
             '18331536916','18633356255','13430827337','13644233798','15294864444','18439131934']