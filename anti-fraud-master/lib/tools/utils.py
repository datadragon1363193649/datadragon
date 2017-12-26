#-*- encoding: utf-8 -*-

import simplejson as json
import hashlib

def dump_json(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=4)

def md5(string):
    m = hashlib.md5()
    m.update(string)
    return m.hexdigest()

if __name__ == '__main__':
    print md5('hello')
