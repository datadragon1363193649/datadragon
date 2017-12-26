#-*- encoding: utf-8 -*-

import web
import time

from utils import dump_json, md5

class HttpJsonError(web.HTTPError):
    status_map = {
        400: 'Bad Request',
        401: 'Unauthorized',
        404: 'Not Found',
        500: 'Interal Error',
        610: 'DB Error',
        602: 'Get Info From Db Error',
        603: 'Get Info From MgDb Error',
        700: 'Data Error',
        
    }
    def __init__(self, status, msg=None, headers={}):
        code = status
        if status > 500: 
            status = 500
        if msg is None:
            msg = self.status_map[status]
        status_string = '%d %s' % (status, self.status_map[status])
        data = dump_json({
            'status': status,
            'code': code,
            'message': msg,
        })
        web.HTTPError.__init__(self, status_string, headers, data)

