#-*- encoding: utf-8 -*-

import web
import datetime
import simplejson as json

from http_exp import HttpJsonError
from common.jklogger import JKLOG

def get_query(key):
    value = web.input().get(key)
    if value is None:
        JKLOG.Error([('x', 'query_404'), ('key', key), ('msg', "no \'%s\' in query" % key)])
        raise HttpJsonError(400, "no \'%s\' in query" % key)
    return value

def get_int_query(key):
    value = get_query(key)
    try:
        return int(value)
    except ValueError:
        JKLOG.Error([('x', 'int_query_f'), ('key', key), ('value',value)])
        raise HttpJsonError(400, "\'%s\' is not integer" % key)

def get_bool_query(key):
    value = get_query(key).lower()
    if value in ['true', '1']:
        return True
    elif value in ['false', '0']:
        return False
    else:
        JKLOG.Error([('x', 'bool_query_f'), ('key', key), ('value', value)])
        raise HttpJsonError(400, '{"msg":"\'%s\' is not bool"}' % key)

def get_list_query(key):
    value = get_query(key)
    if len(value) == 0:
        return []
    return value.split(',')

def get_body_json():
    content_type = web.ctx.env.get('CONTENT_TYPE')
    if content_type is not None and content_type.startswith('application/x-www-form-urlencoded'):
        data = web.input().get('data')
    else:
        data = web.data()
    try:
        return json.loads(data)
    except ValueError:
        JKLOG.Error([('x', 'json_data_f'), ('data', data)])
        raise HttpJsonError(400, '{"msg":"body is not json"}')

def get_or_400(data, key):
    value = data.get(key)
    if value is None:
        JKLOG.Error([('x', 'json_data_404'), ('key', key), ('data', data)])
        raise HttpJsonError(400, '{"msg":"no \'%s\' in body"}' % key)
    return value

def to_json(model, fields=None):
    """
    将对象转为json，目前只是针对SQLAlchemy的对象
    fields是list，元素为string或tuple(string, fields)，例如：
    ['k1', 'k2', ('k3', ['kk1', 'kk2'])]
    """
    if model is None or isinstance(model, (str, unicode, int, long, float, bool)):
        return model
    if isinstance(model, (list, set)):
        return map(lambda x: to_json(x, fields), model)
    if isinstance(model, (datetime.date, datetime.datetime)):
        return model.strftime('%F')
    result = {}
    if fields is None:
        for k, v in model.__dict__.items():
            if not k.startswith('_'):
                result[k] = to_json(v)
    else:
        for item in fields:
            if isinstance(item, (str, unicode)):
                result[item] = to_json(model.__dict__.get(item))
            else:
                k, f = item
                v = model.__dict__.get(k)
                if isinstance(v, (list, set)):
                    result[k] = map(lambda x: to_json(x, f), v)
                else:
                    result[k] = to_json(v, f)
    return result
