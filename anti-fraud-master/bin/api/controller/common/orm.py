#-*- encoding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from config.db_conf import DB_USER_BASIC_INFO, IS_DB_ECHO

Base = declarative_base()

class Orm:
    """ 非线程安全 """
    db_user_basic_info = None

    def __del__(self):
        self.close()

    def get_db_user_baisc_info(self):
        if self.db_user_basic_info is None:
            self.db_user_basic_info = sessionmaker(
                bind=create_engine(DB_USER_BASIC_INFO, convert_unicode=True, echo=IS_DB_ECHO)
            )()
        return self.db_user_basic_info

    def close(self):
        for db in [self.db_user_basic_info]:
            if db is not None:
                db.close()
