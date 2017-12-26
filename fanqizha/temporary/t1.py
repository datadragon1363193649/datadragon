#! /usr/bin/env python
import sys
import os
# import aam

KIND = 200
#BASE = ord('a')

class Node():
    static = 0
    def __init__(self):
        self.fail = None
        self.next = [None]*KIND
        self.end = False
        self.word = None
        self.who = {}
        Node.static += 1

class AAM():
    def __init__(self):
        self.root = Node()
        self.queue = []

    def getIndex(self,char):
        return ord(char)# - BASE

    #insert keywords into trie tree
    def insert(self, string, par):
        p = self.root
        for char in string:
            index = int(char)
            # index = self.getIndex(char)
            # print index
            if p.next[index] == None:
                p.next[index] = Node()
            p = p.next[index]
        p.end = True
        p.who[par] = 0
        p.word = string
        # print p
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
        result2 = []
        p = self.root
        # print p
        i=0
        for char in string:
            index=int(char)
            # index = self.getIndex(char)
            print index
            print p.next[index]
            # print index
            while p.next[index]==None and p!=self.root:
                p=p.fail

            if p.next[index]==None:
                p=self.root
            else:
                p=p.next[index]
            temp = p
            print i,index
            i+=1
            while(temp != self.root and temp.end):
                result.append(temp.word)
                result2.append(temp.who.keys())
                temp = temp.fail

        return (result, result2)


class UAAM():
    def __init__(self,encoding='utf-8'):
        self.ac = AAM()
        self.encoding = encoding

    def getAcString(self,string):
        string = bytearray(string)
        #string = bytearray(string)
        ac_string = ''
        for byte in string:
            ac_string += chr(byte%16)
            ac_string += chr(byte/16)
        #print ac_string
        return ac_string

    def insert(self,string, par):
        ac_string = self.getAcString(string)
        print string
        self.ac.insert(string, par)

    def build_automation(self):
        self.ac.build_automation()

    def match(self,string):
        result = []
        print string
        ac_string = self.getAcString(string)
        #retcode,ret = self.ac.match(ac_string)
        result = self.ac.match(string)
        #for ret in result:
        #    s = ''
        #    for i in range(len(ret)/2):
        #        s += chr(ord(ret[2*i])+ord(ret[2*i+1])*16)
        #    result_new.append(s)
            #ret = s.decode('utf-8')
            #print ret
        return result

if __name__ == '__main__':
    a = UAAM()
    b=UAAM()
    """
    for j in ['16', '05516713', '69']:
        if j != '':
            a.insert(j, j)
    a.build_automation()
    """
    sfile='/Users/ufenqi/Documents/relation'
    plist=[]
    with open(sfile, 'r') as fp:
        for line in fp:
            p0, _, _, p = line.strip().split(',')
            plist.append(p0)
    for pl in plist:
        if pl.strip() and pl.count('*') * 1.0 / len(pl) >= 0.5:
            continue
        a.insert(pl, pl)

    r, r2 = a.match('15218987297')
    # with open(sfile, 'r') as fp:
    #     for line in fp:
    #         p0, _, _, p = line.strip().split(',')
    #         p = p.split('|')
    #         p.append(p0)
    #         for i in p:
    #             if i.strip() and i.count('*')*1.0/len(i) >= 0.5:
    #                 continue
    #             ii = i.split('*')
    #             for j in ii:
    #                 if j != '':
    #                     a.insert(j, i)
    # a.build_automation()
    #
    # while True:
    #     p = raw_input("please input a phone num: ")
    #     r, r2 = a.match(p)
    #     print 'result: ', r
    #     print 'result2: ', r2
    #     s = {}
    #     result = ''
    #     for i in range(len(r)):
    #         #if len(r[i]) == len(p):
    #         #    result = r[i]
    #         #    break
    #         for d in r2[i]:
    #             if len(d) != len(p):
    #                 continue
    #             s.setdefault(d, 0)
    #             s[d] += 1
    #     s = sorted(s.items(), key = lambda x:x[1], reverse = 1)
    #     s = [i for i in s if i[1] > 1]
    #     if result == '' and len(s) > 0:
    #         result = s[0][0]
    #     print 'result: ', result
    #
