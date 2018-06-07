#! /usr/bin/env python
# -*- encoding: utf-8 -*-


import os
import sys

# _abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
# apath=os.path.split(os.path.realpath(_abs_path))[0]
# sys.path.append(_abs_path)
import traceback
import json
import pymongo as pm
import time
import datetime
import pandas as pd
import offline_db_conf as dconf
import logging
import shutil
import math
import feature_class_replace as fcr
import feature_ceiling as fc
import feature_log_handle as lh
import feature_binning as fb
import feature_dummy_variable as fdv
import train_model as tm
import train_model_pre as tmp
import feature_tocsv as fp

reload(sys)
sys.setdefaultencoding('utf-8')
def train(file_name):
    # 类别型编码
    file_name = fcr.train(file_name)
    # 哑变量
    file_name = fdv.train(file_name)
    # 天花板地板
    file_name = fc.train(file_name)
    # log、归一化
    file_name = lh.train(file_name)
    # 分箱
    fb.train(file_name)
def test(file_name):
    # 类别型编码
    file_name = fcr.test(file_name)
    # 哑变量、天花板地板
    file_name = fc.test(file_name)
    # log、归一化
    file_name=lh.test(file_name)
    # 分箱
    fb.test(file_name)
if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)