#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import pymongo
import time
from pymongo import MongoClient

conn = MongoClient('127.0.0.1', 27017)
db = conn.test
data_set = db.test_set

query_type = input('Input a number(1~4) to execute some type of query:')
query_type = int(query_type)

if query_type == 1:
    print('Type 1:')
    time1 = time.time()
    for item in data_set.find({'S':'ess_Rivera_Snchez'}):
        print(item['P'] + ', ' + item['O'])
    time2 = time.time()
    print("Use %.7f seconds" % (time2 - time1))

if query_type == 2:
    print('Type 2:')
    time1 = time.time()
    for item in data_set.find({'O':'Pueblo_of_Naranjito'}):
        print(item['S'] + ', ' + item['P'])
    time2 = time.time()
    print("Use %.7f seconds" % (time2 - time1))

if query_type == 3:
    print('Type 3:')
    time1 = time.time()
    p1 = data_set.find({'P':'isLeaderOf'})
    p2 = data_set.find({'P':'actedIn'})
    s1_set = set()
    s2_set = set()
    for t in p1:
        s1_set.add(t['S'])
    for t in p2:
        s2_set.add(t['S'])
    print(s1_set.intersection(s2_set))
    time2 = time.time()
    print("Use %.7f seconds" % (time2 - time1))

if query_type == 4:
    print('Type 4:')
    time1 = time.time()
    data = list(data_set.aggregate([
        {'$match':{'O':'Pueblo_of_Naranjito'}},
        {'$group':{'_id':'$S', 'num':{'$sum':1}}}
    ]))
    max_cnt = 0
    for i in data:
        tmp = i['num']
        if tmp >= max_cnt:
            max_cnt = tmp
    for i in data:
        if i['num'] == max_cnt:
            print(i['_id'] + ', ' + str(max_cnt))
    time2 = time.time()
    print("Use %.7f seconds" % (time2 - time1))