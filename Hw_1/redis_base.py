#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-4-3 23:49:35

import csv
import redis
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
        print(row)
        record = deepcopy(row)
        record.pop('id')
        r.hmset(row['id'], record)

#测试数据是否存入成功
keys = r.keys() #得到所有hash对象的key
cnt = 0
for key in keys:
    cnt += 1
print('Insert '+ str(cnt) + ' Records: OK')
#打印出id为mat1的学生的所有信息
print(r.hgetall('mat1'))

#测试增加数据项
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
#将一条新的学生记录存入数据库
r.hmset('por650', new_student)
#打印新存入学生的所有信息
print(r.hgetall('por650'))
print('Insert a Record: OK')

keys = r.keys()

#测试修改数据项
cnt = 0
for key in keys:
    if r.hget(key, 'sex') == 'F':
        r.hset(key, 'G3', '100')
        cnt += 1
print('Update ' + str(cnt) + ' Records: OK')

#测试查询数据项
cnt = 0
grade = set()
for key in keys:
    if r.hget(key, 'sex') == 'F':
        grade.add(r.hget(key, 'G3'))
        cnt += 1
print(grade)
print('Find ' + str(cnt) + ' Records: OK')

#测试删除数据项
cnt = 0
for key in keys:
    if int(r.hget(key, 'G3')) == 100:
        r.delete(key)
        cnt += 1
print('Delete ' + str(cnt) + ' Records: OK')
cnt = 0
for key in keys:
    if r.hget(key, 'G3') == '100':
        cnt += 1
print('Find ' + str(cnt) + ' Records: OK')
