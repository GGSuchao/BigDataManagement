#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-4-3 23:49:35

import csv
import redis
import time
from random import randint
from copy import deepcopy

#连接redis
r = redis.Redis(host = '127.0.0.1', port = 6379, db = 0)
if r.ping():
    print('Connect to Redis: Success')

#清空已有的数据库
r.flushall()

#读入student.csv，将数据存入数据库
with open('student.csv', 'r') as csvfile:
    student = csv.DictReader(csvfile, delimiter = ';')
    for row in student:
        #将row拷贝到record中，record中不包含id属性
        record = deepcopy(row)
        record.pop('id')
        r.hmset(row['id'], record)

new_student = {
    'school': 'GP',
    'sex': 'F',
    'age': '14',
    'address': 'U',
    'famsize': 'LE3',
    'Pstatus': 'T',
    'Medu': '2',
    'Fedu': '1',
    'Mjob': 'other',
    'Fjob': 'services',
    'reason': 'reputation',
    'guardian': 'other', 
    'traveltime': '10',
    'studytime': '666',
    'failures': '0',
    'schoolsup': 'yes',
    'famsup': 'no',
    'paid': 'yes',
    'activities': 'no',
    'nursery': 'yes',
    'higher': 'no',
    'internet': 'yes', 
    'romantic': 'yes', 
    'famrel': '5',
    'freetime': '0', 
    'goout': '1', 
    'Dalc': '1', 
    'Walc': '2',
    'health': '3',
    'absences': '11',
    'G1': '0',
    'G2': '0',
    'G3': '0'
}

t0 = time.time()
hash_key = 649
for i in range(1, 10001):
    hash_key += 1 
    hash_key_str = 'por' + str(hash_key)
    new_student['G1'] = str(randint(0, 100))
    #将一条新的学生记录存入数据库
    r.hmset(hash_key_str, new_student)
t1 = time.time()
print('Insert 10000 Records: ' + str(t1 - t0) + 's')

keys = r.keys()

cnt = 0
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    for key in keys:
        if int(r.hget(key, 'G1')) == targ:
            r.hset(key, 'G3', str(59))
            cnt += 1
t1 = time.time()
print('Update ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')

cnt = 0
grade = set()
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    for key in keys:
        if int(r.hget(key, 'G1')) == targ:
            grade.add(r.hget(key, 'G3'))
            cnt += 1
t1 = time.time()
print('Find ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')

cnt = 0
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    keys = r.keys()
    for key in keys:
        if int(r.hget(key, 'G3')) < 60 and int(r.hget(key, 'G1')) == targ:
            r.delete(key)
            cnt += 1
t1 = time.time()
print('Delete ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')
