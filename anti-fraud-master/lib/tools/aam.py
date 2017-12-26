#! /usr/bin/env python
# -*-coding=UTF-8-*-
#
# Date: 2016/05/05
# Desc: key data structs including ac automation machine
#
#

import sys

KIND = 200
#BASE = ord('a')

class Node():
    static = 0
    def __init__(self):
        self.fail = None
        self.next = [None]*KIND
        self.end = False
        self.word = None
        self.pid = -1
        Node.static += 1

class AAMachine():
    def __init__(self):
        self.root = Node()
        self.queue = []

    def getIndex(self,char):
        return ord(char)# - BASE

    #insert keywords into trie tree
    def insert(self, string, pid):
        p = self.root
        for char in string:
            index = self.getIndex(char)
            if p.next[index] == None:
                p.next[index] = Node()
            p = p.next[index]
        p.end = True
        p.pid = pid
        p.word = string

    def build_automation(self):
        self.root.fail = None
        self.queue.append(self.root)
        while len(self.queue)!=0:
            parent = self.queue[0]
            self.queue.pop(0)
            for i,child in enumerate(parent.next):
                if child == None:
                    continue
                if parent == self.root:
                    child.fail = self.root
                else:
                    failp = parent.fail
                    while failp != None and failp.next[i] == None:
                        failp = failp.fail
                    if failp==None:
                        child.fail=self.root
                    else:
                        child.fail = failp.next[i]

                self.queue.append(child)

    def match(self,string):
        result = []
        p = self.root
        for char in string:
            index = self.getIndex(char)
            while p.next[index]==None and p!=self.root:
                p=p.fail

            if p.next[index]==None:
                p=self.root
            else:
                p=p.next[index]
            temp = p
            while(temp != self.root and temp.end):
                result.append(temp.pid)
                temp = temp.fail

        return result

class UAAMachine():
    def __init__(self,encoding='utf-8'):
        self.ac = AAMachine()
        self.encoding = encoding

    def getAcString(self,string):
        string = bytearray(string.encode(self.encoding))
        #string = bytearray(string)
        ac_string = ''
        for byte in string:
            ac_string += chr(byte%16)
            ac_string += chr(byte/16)
        #print ac_string
        return ac_string

    def insert(self,string, pid):
        if type(string) != unicode:
            raise Exception('UnicodeAcAutomation:: insert type not unicode')
        ac_string = self.getAcString(string)
        self.ac.insert(ac_string, pid)

    def build_automation(self):
        self.ac.build_automation()

    def match(self,string):
        result = []
        if type(string) != unicode:
            raise Exception('UnicodeAcAutomation:: insert type not unicode')
        ac_string = self.getAcString(string)
        #retcode,ret = self.ac.match(ac_string)
        result = self.ac.match(ac_string)
        #for ret in result:
        #    s = ''
        #    for i in range(len(ret)/2):
        #        s += chr(ord(ret[2*i])+ord(ret[2*i+1])*16)
        #    result_new.append(s)
            #ret = s.decode('utf-8')
            #print ret
        return result

