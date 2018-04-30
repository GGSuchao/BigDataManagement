#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], 9042)
session = cluster.connect()
session.execute("create keyspace if not exists mydata with replication = {'class':'SimpleStrategy', 'replication_factor':1}")
session.execute('use mydata')
try:
	session.execute('create table data_set(S varchar, P varchar, O varchar, primary key(S, P, O))')
except:
	pass

cnt = 0
with open('yagoThreeSimplified.txt') as f:
    for line in f:
        if line[-3] != ' ':
            s = {}
            s['S'] = line.split(' ')[0]
            s['P'] = line.split(' ')[1]
            s['O'] = line.split(' ')[2]
            cnt += 1
            rec_str = "'" + s['S'] + "'," + "'" + s['P'] + "'," + "'" + s['O'] + "'"
            session.execute('insert into mydata.data_set(S, P, O) values (' + rec_str + ')')
print('Insert ' + str(cnt) + ' Records Totally')

session.execute('create index on mydata.data_set(P)')
session.execute('create index on mydata.data_set(O)')