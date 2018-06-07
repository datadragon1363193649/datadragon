# coding=utf-8
import random
import datetime


def BF_Match(s, t):
    slen = len(s)
    tlen = len(t)
    if slen >= tlen:
        for k in range(slen - tlen + 1):
            i = k
            j = 0
            while i < slen and j < tlen and s[i] == t[j]:
                i = i + 1
                j = j + 1
            if j == tlen:
                return k
            else:
                continue
    return -1


def KMP_Match_1(s, t):
    slen = len(s)
    tlen = len(t)
    if slen >= tlen:
        i = 0
        j = 0
        next_list = [-2 for i in range(len(t))]
        print next_list
        getNext_1(t, next_list)
        print next_list
        # print next_list
        while i < slen:
            if j == -1 or s[i] == t[j]:
                i = i + 1
                j = j + 1
            else:
                j = next_list[j]
            if (j == tlen):
                return i - tlen
    return -1


def KMP_Match_2(s, t):
    slen = len(s)
    tlen = len(t)
    if slen >= tlen:
        i = 0
        j = 0
        next_list = [-2 for i in range(len(t))]
        getNext_2(t, next_list)
        # print next_list
        while i < slen:
            if j == -1 or s[i] == t[j]:
                i = i + 1
                j = j + 1
            else:
                j = next_list[j]
            if j == tlen:
                return i - tlen
    return -1


def getNext_1(t, next_list):
    next_list[0] = -1
    j = 0
    k = -1
    while j < len(t) - 1:
        if k == -1 or t[j] == t[k]:
            j = j + 1
            k = k + 1
            next_list[j] = k
        else:
            k = next_list[k]


def getNext_2(t, next_list):
    next_list[0] = -1
    next_list[1] = 0
    for i in range(2, len(t)):
        tmp = i - 1
        for j in range(tmp, 0, -1):
            if equals(t, i, j):
                next_list[i] = j
                break
            next_list[i] = 0


def equals(s, i, j):
    k = 0
    m = i - j
    while k <= j - 1 and m <= i - 1:
        if s[k] == s[m]:
            k = k + 1
            m = m + 1
        else:
            return False
    return True


def rand_str(length):
    str_0 = []
    for i in range(length):
        str_0.append(random.choice("abcdefghijklmnopqrstuvwxyz"))
    return str_0


def main():
    x = rand_str(40000)
    y = rand_str(10)

    print "The String X Length is : ", len(x), " String is :",
    for i in range(len(x)):
        print x[i],
    print ""
    print "The String Y Length is : ", len(y), " String is :",
    for i in range(len(y)):
        print y[i],
    print ""

    time_1 = datetime.datetime.now()
    pos_1 = BF_Match(x, y)
    time_2 = datetime.datetime.now()
    print "pos_1 = ", pos_1

    time_3 = datetime.datetime.now()
    pos_2 = KMP_Match_1(x, y)
    time_4 = datetime.datetime.now()
    print "pos_2 = ", pos_2

    time_5 = datetime.datetime.now()
    pos_3 = KMP_Match_2(x, y)
    time_6 = datetime.datetime.now()
    print "pos_3 = ", pos_3

    print "Function 1 spend ", time_2 - time_1
    print "Function 2 spend ", time_4 - time_3
    print "Function 3 spend ", time_6 - time_5
#朴素匹配
def naive_match(s, p):
    m = len(s); n = len(p)
    for i in range(m-n+1):#起始指针i
        if s[i:i+n] == p:
            return True
    return False
#KMP
def kmp_match(s, p):
    m = len(s); n = len(p)
    cur = 0#起始指针cur
    table = partial_table(p)
    while cur<=m-n:
        for i in range(n):
            if s[i+cur]!=p[i]:
                cur += max(i - table[i-1], 1)#有了部分匹配表,我们不只是单纯的1位1位往右移,可以一次移动多位
                # print cur
                break
        else:
            # print cur,m-n
            return 0
    return False

#部分匹配表
def partial_table(p):
    '''partial_table("ABCDABD") -> [0, 0, 0, 0, 1, 2, 0]'''
    prefix = set()
    postfix = set()
    ret = [0]
    for i in range(1,len(p)):
        print p
        prefix.add(p[:i])
        postfix = {p[j:i+1] for j in range(1,i+1)}
        print 'prefix',prefix
        print 'postfix',postfix
        ret.append(len((prefix&postfix or {''}).pop()))
    print ret
    return ret
if __name__ == '__main__':
    x = rand_str(40000)
    y = rand_str(10)

    print "The String X Length is : ", len(x)
    x=''.join(x)
    print x
    print ""
    print "The String Y Length is : ", len(y)
    y=''.join(y)
    print y
    print ""

    time_1 = datetime.datetime.now()
    print naive_match("BBC ABCDAB ABCDABCDABDE", "ABCDABD")
    time_2 = datetime.datetime.now()

    time_3 = datetime.datetime.now()
    print kmp_match("BBC ABCDAB ABCDABCDABDE", "ABCDABD")
    time_4 = datetime.datetime.now()

    print "Function 1 spend ", time_2 - time_1
    print "Function 2 spend ", time_4 - time_3




