# -*- encoding: utf-8 -*-
import offline_db_conf as dconf
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
class featureinfor(object):
    def __init__(self):
        self.mysqlhost = dconf.mysql_host_test
        self.mysqlport = dconf.mysql_port_test
        self.mysqluser = dconf.mysql_user_test
        self.mysqlpasswd = dconf.mysql_passwd_test
        self.mysqldb = dconf.mysql_db_fea
        self.mysqlconn = None
        self.init_mysql_conn()
    def init_mysql_conn(self):
        self.mysqlconn= MySQLdb.connect(host=self.mysqlhost,
                                        port=self.mysqlport, user=self.mysqluser,
                                        passwd=self.mysqlpasswd,
                               db=self.mysqldb, charset="utf8")
    def get_infor(self):
        mysqlcursor = self.mysqlconn.cursor()
        sql = 'select * from feature_information where isuse=0'
        n = mysqlcursor.execute(sql)
        # logging.info('The number of %s is :%s', tag, n)
        bphn = []
        if n != 0:
            phncontent = mysqlcursor.fetchall()
            print len(phncontent)
            # self.put_data_tag(passcontent, 'is_pass')
            i = 0
            buffer = []
            for fli in phncontent:
                print fli

if __name__ == '__main__':
    f=featureinfor()
    f.get_infor()