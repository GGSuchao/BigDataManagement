#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], 9042)
session = cluster.connect()

query_type = input('Input a number(1~4) to execute some type of query:')
query_type = int(query_type)

if query_type == 1:
	print('Type 1 with optimize:')
	time1 = time.time()
	rs = session.execute("select * from mydata.data_set where S = 'ess_Rivera_Snchez'")
	for i in rs:
		print(i[1] + ',' + i[2])
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

if query_type == 2:
	print('Type 2 with optimize:')
	time1 = time.time()
	rs = session.execute("select * from mydata.data_set where O = 'Pueblo_of_Naranjito\r\n'")
	for i in rs:
		print(i.s + ', ' + i.p)
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

if query_type == 3:
	print('Type 3 with optimize:')
	time1 = time.time()
	p1 = session.execute("select S from mydata.data_set where P = 'isLeaderOf'")
	p2 = session.execute("select S from mydata.data_set where P = 'actedIn'")
	s1_set = set()
	s2_set = set()
	for t in p1:
		s1_set.add(t.s)
	for t in p2:
		s2_set.add(t.s)
	print(s1_set.intersection(s2_set))
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

if query_type == 4:
	print('Type 4 with optimize:')
	time1 = time.time()
	rs = session.execute("select S from mydata.data_set where O = 'Pueblo_of_Naranjito\r\n'")
	s_list = []
	for i in rs:
		if i.s not in s_list:
			s_list.append({'name':i.s, 'cnt':1}) 
		else:
			index = s_list.index(i.s)
			s_list[index]['cnt'] += 1
	max_cnt = 0
	for i in s_list:
		if i['cnt'] >= max_cnt:
			max_cnt = i['cnt']
	for i in s_list:
		if (i['cnt'] == max_cnt):
			print(i['name'] + ', ' + str(max_cnt))
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))