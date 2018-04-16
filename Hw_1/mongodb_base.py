#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-4-13 23:49:35

import pymongo
import csv
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

#测试数据是否存入成功
print('Insert '+ str(student.count()) + ' Records: OK')
print(student.find_one({'id':'mat1'}))

#测试增加数据项
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
student.insert(new_student)
#打印新存入学生的所有信息
print(student.find_one({'studytime':666}))
print('Insert a Record: OK')

#测试修改数据项
student.update({'sex':'F'}, {'$set':{'G3':100}}, multi = True)
print('Update ' + str(student.find({'G3':100}).count()) + ' Records: OK')

#测试查询数据项
sex = set()
cnt = 0
ret_list = student.find({'G3':{'$gt':99}})
for record in ret_list:
    sex.add(record['sex'])
    cnt += 1
print(sex)
print('Find ' + str(cnt) + ' Records: OK')

#测试删除数据项
cnt = student.find({'G3':{'$gte':100}}).count()
student.remove({'G3':{'$gte':100}})
print('Delete ' + str(cnt) + ' Records: OK')
cnt = student.find({'G3':{'$gte':100}}).count()
print('Find ' + str(cnt) + ' Records: OK')
