#! /usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import sys
import time
from http_exp import HttpJsonError
import controller.common.utils as ctrl_utils
from controller.risk.anti_fraud import *
from common.jklogger import JKLOG

class Risk(object):
    def __init__(self):
        #self.af = AntiFraud()
        pass

    def GET(self):
        """
        code:
        100-500: 系统码
        600-699: 各种不存在
        700-799: 数据错误
        """
        code = 200 # 其他的从600开始, 一组逻辑上相同的业务错误在100之内
        phn = ctrl_utils.get_query('phn_num')
        app_type = ctrl_utils.get_int_query('app_type')
        v = ctrl_utils.get_int_query('ver')
        JKLOG.Info([('x', 'request'), ('phn_num', phn), ('app_type', app_type), ('version', v)])
        if not web.af.is_phn_exist(phn):
            JKLOG.Info([('phn_num', phn), ('app_type', app_type), ('info', 'phnoe num dose not exist')])
            raise HttpJsonError(600, 'phone num dose not exist')
        #p = self.af.get_score(phn, app_type)
        p = web.af.get_score(phn, app_type, v)
        #time.sleep(5)
        JKLOG.Info([('x', 'response'), ('phn_num', phn), ('app_type', app_type), ('score', p)])
        return {
            'data':{
                'phn_num': phn,
                'app_type': app_type,
                'prob': p,
                'ver': v
            },
            'code': code,
        }
