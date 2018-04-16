#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-4-3 23:49:35

import csv
import time
from random import randint
from cassandra.cluster import Cluster

#连接数据库
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("drop keyspace bigdata")
session.execute("create keyspace bigdata with replication = {'class':'SimpleStrategy', 'replication_factor':1}")
session.execute("use bigdata")
try:
    session.execute("create table student (id varchar primary key, school varchar, sex varchar, age int, address varchar, famsize varchar, Pstatus varchar, Medu int, Fedu int, Mjob varchar, Fjob varchar, reason varchar, guardian varchar, traveltime int, studytime int, failures int, schoolsup varchar, famsup varchar, paid varchar, activities varchar, nursery varchar, higher varchar, internet varchar, romantic varchar, famrel int, freetime int, goout int, Dalc int, Walc int, health int, absences int, G1 int, G2 int, G3 int)")
except:
    pass

#读入student.csv，将数据存入数据库
with open('student.csv', 'r') as csvfile:
    records = csv.DictReader(csvfile, delimiter = ';')
    for record in records:
        str_record = record['id'] + "','" + record['school'] + "','" + record['sex'] + "'," + record['age'] + ",'" + record['address'] + "','" + record['famsize'] + "','" + record['Pstatus'] + "'," + record['Medu'] + "," + record['Fedu'] + ",'" + record['Mjob'] + "','" + record['Fjob'] + "','" + record['reason'] + "','" + record['guardian'] + "'," 
        str_record = str_record + record['traveltime'] + "," + record['studytime'] + "," + record['failures'] + ",'" + record['schoolsup'] + "','" + record['famsup'] + "','" + record['paid'] + "','" + record['activities'] + "','" + record['nursery'] + "','" + record['higher'] + "','" + record['internet'] + "','" + record['romantic'] + "'," + record['famrel']
        str_record = str_record + "," + record['freetime'] + "," + record['goout'] + "," + record['Dalc'] + "," + record['Walc'] + "," + record['health'] + "," + record['absences'] + "," + record['G1'] + "," + record['G2'] + "," + record['G3']
        session.execute("insert into bigdata.student(id, school, sex, age, address, famsize, Pstatus, Medu, Fedu, Mjob, Fjob, reason, guardian, traveltime, studytime, failures, schoolsup, famsup, paid, activities, nursery, higher, internet, romantic, famrel, freetime, goout, Dalc, Walc, health, absences, G1, G2, G3) values('" + str_record + ")")

t0 = time.time()
id_num = 649
for i in range(1, 10001):
    id_num += 1 
    id_str = 'por' + str(id_num)
    record_str = "'" + id_str + "','GP','F',14,'U','LE3','T',2,1,'other','services','reputation','other',10,666,0,'yes','no','yes','no','yes','no','yes','yes',5,0,1,1,2,3,11," + str(randint(0, 100)) + ",0,0"
    session.execute("insert into bigdata.student(id, school, sex, age, address, famsize, Pstatus, Medu, Fedu, Mjob, Fjob, reason, guardian, traveltime, studytime, failures, schoolsup, famsup, paid, activities, nursery, higher, internet, romantic, famrel, freetime, goout, Dalc, Walc, health, absences, G1, G2, G3) values(" + record_str + ")")
t1 = time.time()
print('Insert 10000 Records: ' + str(t1 - t0) + 's')

cnt = 0
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    rs = session.execute("select * from bigdata.student")
    for i in rs:
        if i.g1 == targ:
            session.execute("update bigdata.student set G3=59 where id='" + i.id + "'")
            cnt += 1
t1 = time.time()
print('Update ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')

print('Find by the primary key:')
cnt = 0
grade = set()
t0 = time.time()
for i in range(1, 10001):
    targ = randint(1, 10000)
    rs = session.execute("select * from bigdata.student where id ='por" + str(targ + 649) + "'")
    for i in rs:
        cnt += 1
t1 = time.time()
print('Find ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')

print('Find by the other key:')
cnt = 0
grade = set()
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    rs = session.execute("select * from bigdata.student")
    for i in rs:
        if i.g1 == targ:
            grade.add(i.g3)
            cnt += 1
t1 = time.time()
print('Find ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')

cnt = 0
t0 = time.time()
for i in range(1, 100):
    targ = randint(0, 100)
    rs = session.execute("select * from bigdata.student")
    for i in rs:
        if i.g3 < 60 and i.g1 == targ:
            session.execute("delete from bigdata.student where id='" + i.id + "'")
            cnt += 1
t1 = time.time()
print('Delete ' + str(cnt) + ' Records: ' + str(t1 - t0) + 's')
