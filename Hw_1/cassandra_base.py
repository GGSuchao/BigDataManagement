#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-4-3 23:49:35

import csv
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

#测试数据是否存入成功
cnt = 0
rs = session.execute("select * from bigdata.student")
for i in rs:
    cnt += 1
print('Insert '+ str(cnt) + ' Records: OK')
#打印出id为mat1的学生的所有信息
for i in rs:
    if (i.id == 'mat1'):
        print(i)

#测试增加数据项
#将一条新的学生记录存入数据库
session.execute("insert into bigdata.student(id, school, sex, age, address, famsize, Pstatus, Medu, Fedu, Mjob, Fjob, reason, guardian, traveltime, studytime, failures, schoolsup, famsup, paid, activities, nursery, higher, internet, romantic, famrel, freetime, goout, Dalc, Walc, health, absences, G1, G2, G3) values('por650','GP','F',14,'U','LE3','T',2,1,'other','services','reputation','other',10,666,0,'yes','no','yes','no','yes','no','yes','yes',5,0,1,1,2,3,11,0,0,0)")
#打印新存入学生的所有信息
rs = session.execute("select * from bigdata.student where id='por650'")
print(rs)
print('Insert a Record: OK')

#测试修改数据项
cnt = 0
rs = session.execute("select * from bigdata.student")
for i in rs:
    if (i.sex == 'F'):
        session.execute("update bigdata.student set G3=100 where id='" + i.id + "'")
        cnt += 1
print('Update ' + str(cnt) + ' Records: OK')

#测试查询数据项
cnt = 0
grade = set()
rs = session.execute("select * from bigdata.student")
for i in rs:
    if (i.sex == 'F'):
        grade.add(i.g3)
        cnt += 1
print(grade)
print('Find ' + str(cnt) + ' Records: OK')

#测试删除数据项
cnt = 0
rs = session.execute("select * from bigdata.student")
for i in rs:
    if (i.g3 == 100):
        session.execute("delete from bigdata.student where id='" + i.id + "'")
        cnt += 1
print('Delete ' + str(cnt) + ' Records: OK')
cnt = 0
rs = session.execute("select * from bigdata.student")
for i in rs:
    if (i.g3 == 100):
        cnt += 1
print('Find ' + str(cnt) + ' Records: OK')
