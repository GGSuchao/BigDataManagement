#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], 9042)
session = cluster.connect()


query_type = input('Input a number(1~4) to execute some type of query:')
query_type = int(query_type)

if query_type == 1:
	print('Type 1:')
	time1 = time.time()
	rs = session.execute("select * from mydata.data_set where S = 'ess_Rivera_Snchez'")
	for i in rs:
		print(i.p + ', ' + i.o)
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

if query_type == 2:
	print('Type 2:')
	time1 = time.time()
	rs = session.execute("select * from mydata.data_set")
	for i in rs:
		if i.o == 'Pueblo_of_Naranjito\r\n':
			print(i.s + ', ' + i.p)
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

if query_type == 3:
	print('Type 3:')
	time1 = time.time()
	p1 = session.execute("select * from mydata.data_set")
	s1_set = set()
	s2_set = set()
	for t in p1:
		if t.p == 'isLeaderOf':
			s1_set.add(t.s)
	p1 = session.execute("select * from mydata.data_set")
	for t in p1:
		if t.p == 'actedIn':
			s2_set.add(t.s)
	print(s1_set.intersection(s2_set))
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

if query_type == 4:
	print('Type 4:')
	time1 = time.time()
	rs = session.execute("select * from mydata.data_set")
	s_list = []
	for i in rs:
		if i.o == 'Pueblo_of_Naranjito\r\n':
			if (i.s not in s_list):
				s_list.append({'name':i.s, 'cnt':1})
			else:
				index = s_list.index(i.s)
				s_list[index]['cnt'] += 1
	max_cnt = 0
	for s in s_list:
		if s['cnt'] >= max_cnt:
			max_cnt = s['cnt']
	for s in s_list:
		if s['cnt'] == max_cnt:
			print(s['name'] + ', ' + str(max_cnt))
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

