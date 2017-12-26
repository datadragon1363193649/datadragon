#!/usr/bin/python
#-*- encoding: utf-8 -*-

import os
import sys
import web

BIN_DIR = os.path.dirname(__file__)
sys.path.append(BIN_DIR)
sys.path.append(os.path.join(BIN_DIR, '../../config'))
sys.path.append(os.path.join(BIN_DIR, '../../lib'))
sys.path.append(os.path.join(BIN_DIR, '../../lib/tools'))

import common_conf as conf
import db_conf as dconf
from http_exp import HttpJsonError
from tools.utils import dump_json

from common.jklogger import JKLOG
from controller.risk.jk import Risk
from controller.risk.anti_fraud import AntiFraud

JKLOG.Initialize(server_name = 'risk',
                 fname = os.path.join(BIN_DIR, '../../log/risk'),
                 log_type = conf.LOG_TYPE,
                 jk_log_level = conf.LOG_LEVEL,
                 ntrvl=1)

web.af = AntiFraud()

urls = (
    '/risk', 'controller.risk.jk.Risk',
)

def processor(handler):
    try:
        JKLOG.Info([('x', 'req'), ('method', web.ctx.method),
                    ('url', web.ctx.fullpath), ('ip', web.ctx.ip)])
        result = handler()
        if isinstance(result, dict):
            result['status'] = int(web.ctx.status.split(' ')[0])
            if not result.has_key('message'):
                result['message'] = 'OK'
            return dump_json(result)
        else:
            return result
    except Exception, e:
        if isinstance(e, HttpJsonError):
            raise e
        elif isinstance(e, web.HTTPError):
            #print web.ctx.status
            status = int(web.ctx.status.split(' ')[0])
            raise HttpJsonError(status, e.data)
        else:
            JKLOG.Error([('x', 'except'), ('except', e)])
            raise HttpJsonError(500, str(e))
    finally:
        pass
        #web.ctx.orm.close()

web.config.debug = False
app = web.application(urls, globals(), autoreload = False)
app.add_processor(processor)
application = app.wsgifunc()

if __name__ == '__main__':
    app.run()
