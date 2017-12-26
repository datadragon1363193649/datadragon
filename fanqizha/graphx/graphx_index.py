#! /usr/bin/env python
# -*- encoding: utf-8 -*-
"""
在已有的顶点文件里添加编号，在关系文件里用编号代替电话
顶点文件格式：(2,13616274217,0);代表的意思为：(编号(从0一直往后累加，不重复)，电话号，是否在黑名单里（0：不是，1:是）)
关系文件格式：(1400971,105049,1);代表的意思为：(编号1，编号2，边权值(暂时不用，所有的都是1))，说明编号1和编号2有联系
"""


import os
import sys

_abs_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
apath=os.path.split(os.path.realpath(_abs_path))[0]
sys.path.append(_abs_path)
# sys.path.append(apath+'/log')
import traceback
import json
import pymongo as pm
import time
import datetime
import offline_db_conf as dconf
import logging
import shutil
reload(sys)
sys.setdefaultencoding('utf-8')
class GraphxIndex(object):
    def __init__(self):
        self.daylist = dconf.daylist
        self.oneday=dconf.oneday
    def one_day(self):
        vertexfile='/home/hadoop/xuyonglong/graphxdata/'+self.oneday+'/vertex'
        vertexidfile='/home/hadoop/xuyonglong/graphxdata/'+self.oneday+'/vertex_id'
        relationfile='/home/hadoop/xuyonglong/graphxdata/'+self.oneday+'/relation'
        relationid='/home/hadoop/xuyonglong/graphxdata/'+self.oneday+'/relation_id'
        vertexidfp=open(vertexidfile,'w')
        relationidfp=open(relationid,'w')
        verdic={}
        with open(vertexfile,'r') as fp:
            num=0
            for line in fp:
                linelist=line.strip().split(',')
                if linelist[0] in verdic:
                    continue
                verdic[linelist[0]]=num
                vertexidfp.write(str(num)+","+",".join(linelist)+"\n")
                num+=1
        relationdic={}
        with open(relationfile,'r') as fp:
            for line in fp:
                linelist=line.strip().split(',')
                if linelist[0] in verdic:
                     if linelist[1] in verdic:
                        relations=str(verdic[linelist[0]])+','+str(verdic[linelist[1]])+',1'
                        relationdic[relations]=0
                        relations = str(verdic[linelist[1]]) + ',' + str(verdic[linelist[0]]) + ',1'
                        relationdic[relations] = 0
        for rel in relationdic:
            relationidfp.write(rel+"\n")
    def all_day(self):
        vertexidfile = "/home/hadoop/xuyonglong/graphxdata/vertex_id_16_18"
        relationidfile = "/home/hadoop/xuyonglong/graphxdata/relation_id_16_18"
        verdic = {}
        wfp1 = open(vertexidfile, 'w')
        wfp2 = open(relationidfile, 'w')
        num = 0
        for days in self.daylist:
            vertexfile='/home/hadoop/xuyonglong/graphxdata/'+days+'/vertex'
            relationfile = '/home/hadoop/xuyonglong/graphxdata/'+days+'/relation'
            try:
                with open(vertexfile, 'r') as fp:
                    for line in fp:
                        linelist = line.strip().split(',')
                        if linelist[0] in verdic:
                            continue
                        verdic[linelist[0]] = num
                        wfp1.write(str(num) + "," + ",".join(linelist) + "\n")
                        num += 1
                with open(relationfile, 'r') as fp:
                    for line in fp:
                        linelist = line.strip().split(',')
                        if linelist[0] in verdic:
                            if linelist[1] in verdic:
                                relations = str(verdic[linelist[0]]) + ',' + str(verdic[linelist[1]]) + ',1'
                                wfp2.write(relations + "\n")
                                relations = str(verdic[linelist[1]]) + ',' + str(verdic[linelist[0]]) + ',1'
                                wfp2.write(relations + "\n")


            except Exception, e:
                print e
            print days
if __name__ == '__main__':
    g=GraphxIndex()
    tat = sys.argv[1]
    if tat == 'one':
        g.one_day()
    if tat == 'all':
        g.all_day()