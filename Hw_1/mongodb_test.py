#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-4-13 23:49:35

import pymongo
import csv
import time
from random import randint
from copy import deepcopy
from pymongo import MongoClient

#连接数据库
client = MongoClient('localhost', 27017)

#选择数据库及集合
db = client.bigdata
student = db.student

#清空已有的集合
db.student.drop()

#读入student.csv，将数据存入数据库
with open('student.csv', 'r') as csvfile:
    documents = csv.DictReader(csvfile, delimiter = ';')
    for document in documents:
        document['age'] = int(document['age'])
        document['Medu'] = int(document['Medu'])
        document['Fedu'] = int(document['Fedu'])
        document['traveltime'] = int(document['traveltime'])
        document['studytime'] = int(document['studytime'])
        document['failures'] = int(document['failures'])
        document['famrel'] = int(document['famrel'])
        document['freetime'] = int(document['freetime'])
        document['goout'] = int(document['goout'])
        document['Dalc'] = int(document['Dalc'])
        document['Walc'] = int(document['Walc'])
        document['health'] = int(document['health'])
        document['absences'] = int(document['absences'])
        document['G1'] = int(document['G1'])
        document['G2'] = int(document['G2'])
        document['G3'] = int(document['G3'])
        student.insert(document)

new_student = {
    'id':'por650',
    'school': 'GP',
    'sex': 'F',
    'age': 14,
    'address': 'U',
    'famsize': 'LE3',
    'Pstatus': 'T',
    'Medu': 2,
    'Fedu': 1,
    'Mjob': 'other',
    'Fjob': 'services',
    'reason': 'reputation',
    'guardian': 'other', 
    'traveltime': 10,
    'studytime': 666,
    'failures': 0,
    'schoolsup': 'yes',
    'famsup': 'no',
    'paid': 'yes',
    'activities': 'no',
    'nursery': 'yes',
    'higher': 'no',
    'internet': 'yes', 
    'romantic': 'yes', 
    'famrel': 5,
    'freetime': 0, 
    'goout': 1, 
    'Dalc': 1, 
    'Walc': 2,
    'health': 3,
    'absences': 11,
    'G1': 0,
    'G2': 0,
    'G3': 0
}

t0 = time.time()
id_num = 649
for i in range(1, 10001):
    id_num += 1 
    id_str = 'por' + str(id_num)
    new_student_1 = deepcopy(new_student)
    new_student_1['id'] = id_str
    new_student_1['G1'] = randint(0, 100)
    student.insert(new_student_1)
t1 = time.time()
print('Insert 10000 Records: ' + str(t1 - t0) + 's')

cnt = 0
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    student.update({'G1':targ}, {'$set':{'G3':59}}, multi = True)
    cnt += student.find({'G1':targ}).count()
t1 = time.time()
print('Update ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')


cnt = 0
grade = set()
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    ret_list = student.find({'G1':targ})
    for record in ret_list:
        grade.add(record['G3'])
        cnt += 1
t1 = time.time()
print('Find ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')

cnt = 0
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    cnt += student.find({'G3':{'$lt':60}, 'G1':targ}).count()
    student.remove({'G3':{'$lt':60}, 'G1':targ})
t1 = time.time()
print('Delete ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')
