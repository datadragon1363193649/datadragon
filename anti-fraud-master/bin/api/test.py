#! /usr/bin/env python

import sys
import json
import requests
import time

def g(pii):
    r = requests.get('http://172.16.51.68:8080/risk?phn_num=%s&app_type=0' % pii)
    print r.text
if __name__ == '__main__':
    p = []
    with open('p', 'r') as fp:
        for line in fp:
            pp = line.strip()
            if pp: p.append(pp)
    #p = ['13967168097']
    for pi in p:
        start = time.clock()
        g(pi)
        
        end = time.clock()
        print end - start
    
    
