#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import pymongo
from pymongo import MongoClient
import time

conn = MongoClient('127.0.0.1', 27017)

db = conn.test
data_set = db.test_set

data_set.drop()
cnt = 0
with open('yagoThreeSimplified.txt', 'r') as f:
    for line in f:
        if line[-3] != ' ':
            s = {}
            s['S'] = line.split()[0]
            s['P'] = line.split()[1]
            s['O'] = line.split()[2]
            data_set.insert(s)
            cnt += 1
print('Insert ' + str(cnt) + ' Records Totally')

