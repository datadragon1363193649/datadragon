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
import feature_distribute as fd
import logHandle as lh
import feature_fenxiang2017max as ff
import feature_indicate as fin
import trainmodel as tm
import trainmodelpre as tmp
import feature_preproccess as fp

reload(sys)
sys.setdefaultencoding('utf-8')
def train(file_name):
    # file_name = fp.traincsv(file_name)
    file_name = fd.train(file_name)
    file_name = lh.train(file_name)
    ff.train(file_name)
def test(file_name):
    # file_name=fp.testcsv(file_name)
    file_name = fd.test(file_name)
    file_name=lh.test(file_name)
    ff.test(file_name)
if __name__ == '__main__':
    tat = sys.argv[1]
    input_file = sys.argv[2]
    if tat == 'train':
        train(input_file)
    if tat == 'test':
        test(input_file)