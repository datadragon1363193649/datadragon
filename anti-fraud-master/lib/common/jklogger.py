# ! /usr/bin/env python
# coding: utf-8
# Logger Class Lib.
# author:
# date: 2016/05/21
# version: 1.0

import os
import sys
import time
import logging
import logging.handlers
import threading
import socket


# from thrift import Thrift
# from thrift.transport import TSocket
# from thrift.transport import TTransport
# from thrift.protocol import TBinaryProtocol
# from scribe import scribe


class Singleton(object):
    __lock = threading.Lock()

    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            Singleton.__lock.acquire()
            if not hasattr(cls, '_instance'):
                org = super(Singleton, cls)
                cls._instance = org.__new__(cls)
            Singleton.__lock.release()
        return cls._instance


class JkLogger(Singleton):
    LEVEL_NONE = 0x00
    LEVEL_INFO = 0x01
    LEVEL_LOG = 0x02
    LEVEL_DEBUG = 0x04
    LEVEL_ERROR = 0x08
    LEVEL_FATAL = 0x10
    LEVEL_ALL = 0xFFFFFFFF
    __logging = None
    LOGING_MAP = {LEVEL_INFO: lambda x: Singleton.__logging.info(x),
                  LEVEL_LOG: lambda x: Singleton.__logging.info(x),
                  LEVEL_DEBUG: lambda x: Singleton.__logging.debug(x),
                  LEVEL_ERROR: lambda x: Singleton.__logging.error(x),
                  LEVEL_FATAL: lambda x: Singleton.__logging.fatal(x)
                  }

    def Initialize(self, server_name, fname="./log", log_type=0, jk_log_level="efil", scribe_ip="127.0.0.1",
                   scribe_port=1464, ntrvl=1, backcount=7):
        self.__server_map = {JkLogger.LEVEL_LOG: server_name + "_l",
                             JkLogger.LEVEL_ERROR: server_name + "_e",
                             JkLogger.LEVEL_FATAL: server_name + "_f"}
        Singleton.__logging = self.__initlog(fname, ntrvl, backcount)
        self.__scribe_ip = scribe_ip
        self.__scribe_port = scribe_port
        self.__log_type = log_type
        if len(jk_log_level) > 0:
            self.__level = 0
            if jk_log_level.find('f') >= 0:
                self.__level |= JkLogger.LEVEL_FATAL
                Singleton.__logging.setLevel(logging.CRITICAL)
            if jk_log_level.find('e') >= 0:
                self.__level |= JkLogger.LEVEL_ERROR
                Singleton.__logging.setLevel(logging.ERROR)
            if jk_log_level.find('i') >= 0:
                self.__level |= JkLogger.LEVEL_INFO
                Singleton.__logging.setLevel(logging.INFO)
            if jk_log_level.find('l') >= 0:
                self.__level |= JkLogger.LEVEL_LOG
                Singleton.__logging.setLevel(logging.INFO)
            if jk_log_level.find('d') >= 0:
                self.__level |= JkLogger.LEVEL_DEBUG
                Singleton.__logging.setLevel(logging.DEBUG)
        else:
            self.__level = JkLogger.LEVEL_INFO | JkLogger.LEVEL_ERROR | JkLogger.LEVEL_FATAL | JkLogger.LEVEL_LOG
            logging.basicConfig(level=logging.INFO)
        # 获取hostname
        self.__host = socket.gethostname()

    # 输出Info日志
    def Info(self, kv):
        if self.IsLog(JkLogger.LEVEL_INFO):
            msg = self.LogHeader()
            for item in kv:
                if len(item) == 2:
                    v = str(item[1]).replace("%3A", "%%3A")
                    v = v.replace(";", "%3A")
                    msg = "%s;%s=%s" % (msg, item[0], v)
            self.__log(JkLogger.LEVEL_INFO, msg)

    # 输出Debuf日志
    def Debug(self, kv):
        if self.IsLog(JkLogger.LEVEL_DEBUG):
            try:
                raise Exception
            except:
                f = sys.exc_info()[2].tb_frame.f_back
            msg = self.LogHeader(f.f_code.co_filename, f.f_lineno)
            for item in kv:
                if len(item) == 2:
                    v = str(item[1]).replace("%3A", "%%3A")
                    v = v.replace(";", "%3A")
                    msg = "%s;%s=%s" % (msg, item[0], v)
            self.__log(JkLogger.LEVEL_DEBUG, msg)

    # 输出Log日志
    def Log(self, kv):
        if self.IsLog(JkLogger.LEVEL_LOG):
            msg = self.LogHeader()
            for item in kv:
                if len(item) == 2:
                    v = str(item[1]).replace("%3A", "%%3A")
                    v = v.replace(";", "%3A")
                    msg = "%s;%s=%s" % (msg, item[0], v)
            self.__log(JkLogger.LEVEL_LOG, msg)

    # 输出Erorr日志
    def Error(self, kv):
        if self.IsLog(JkLogger.LEVEL_ERROR):
            try:
                raise Exception
            except:
                f = sys.exc_info()[2].tb_frame.f_back
            msg = self.LogHeader(f.f_code.co_filename, f.f_lineno)
            for item in kv:
                if len(item) == 2:
                    v = str(item[1]).replace("%3A", "%%3A")
                    v = v.replace(";", "%3A")
                    msg = "%s;%s=%s" % (msg, item[0], v)
            self.__log(JkLogger.LEVEL_ERROR, msg)

    # 输出Fatal日志
    def Fatal(self, kv):
        if self.IsLog(JkLogger.LEVEL_FATAL):
            try:
                raise Exception
            except:
                f = sys.exc_info()[2].tb_frame.f_back
            msg = self.LogHeader(f.f_code.co_filename, f.f_lineno)
            for item in kv:
                if len(item) == 2:
                    v = str(item[1]).replace("%3A", "%%3A")
                    v = v.replace(";", "%3A")
                    msg = "%s;%s=%s" % (msg, item[0], v)
            self.__log(JkLogger.LEVEL_FATAL, msg)

    # 设置LogLevel
    def EnableLog(self, level):
        self.__level |= level

    def DisableLog(self, level):
        self.__level &= ~level

    def IsLog(self, level):
        return (self.__level & level) != 0

    def SetLevel(self, level):
        self.__level = level

    def GetLevel(self):
        return self.__level

    # 获取日志头
    def LogHeader(self, f=None, line=0):
        if f and f.rfind("/") > 0:
            f = f[f.rfind("/") + 1:]
        tm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        if f:
            return "tid=%s;h=%s;t=%s;f=%s;l=%u" % (threading.currentThread().ident, self.__host, tm, f, line)
        else:
            return "tid=%s;h=%s;t=%s" % (threading.currentThread().ident, self.__host, tm)

    # 生成日志对象
    def __initlog(self, fname, ntrvl, backcount):
        hdlr = logging.handlers.TimedRotatingFileHandler(filename=fname, when='D', interval=ntrvl,
                                                         backupCount=backcount)
        fmt = "[%(asctime)s] [%(levelname)s] " \
              "\[ %(filename)s:%(lineno)s - %(name)s ] %(message)s "
        formatter = logging.Formatter(fmt)
        hdlr.setFormatter(formatter)
        logger = logging.getLogger()
        logger.addHandler(hdlr)
        return logger

    # 打印日志
    def __log(self, level, msg):
        if 0 != self.__log_type:
            # 写日志到本地
            JkLogger.LOGING_MAP[level](msg)
        """
            if 1 != self.__log_type and level != JkLogger.LEVEL_DEBUG and level != JkLogger.LEVEL_INFO :
                #写到scribe
                try:
                    msg = "%s\n" % msg
                    transport = TSocket.TSocket(host=self.__scribe_ip, port=self.__scribe_port)
                    transport = TTransport.TFramedTransport(transport)
                    protocol = TBinaryProtocol.TBinaryProtocol(transport)
                    client = scribe.Client(iprot=protocol, oprot=protocol)
                    transport.open()
                    log_entry = scribe.LogEntry(category=self.__server_map[level], message=msg)
                    result = client.Log(messages=[log_entry])
                except Thrift.TException, tx:
                    JkLogger.LOGING_MAP[JkLogger.LEVEL_ERROR]("Fail to write log to [%s:%d] error :%s" % (self.__scribe_ip, self.__scribe_port, tx.message))
                finally:
                    transport.close()
        """


JKLOG = JkLogger()

if __name__ == "__main__":
    def test():
        JKLOG.Info([("k1", "v1")])
        JKLOG.Info([("k1", "v1"), ("k2", "v2")])
        JKLOG.Debug([("k1", "v1"), ("k2", "v2"), ("k3", "v3")])
        JKLOG.Log([("k1", "v1"), ("k2", "v2")])
        JKLOG.Error([("k1", "v1"), ("k2", "v2")])
        JKLOG.Fatal([("k1", "v1"), ("k2", "v2")])

    JKLOG.Initialize('111',fname="/Users/ufenqi/Downloads/dltest", log_type=1, jk_log_level="efil", scribe_ip="127.0.0.1", scribe_port=1464, ntrvl=1,
                     backcount=7)
    test()