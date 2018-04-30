#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import time
import redis

#连接redis
r = redis.Redis(host = '127.0.0.1', port = 6379, db = 0)
if r.ping():
    print('Connect to Redis: Success')

query_type = input('Input a number(1~4) to execute some type of query:')
query_type = int(query_type)

#给定一个si，给出它所有的P和O，<si, P, O>
if query_type == 1:
	print('Type 1 with optimize:')
	time1 = time.time()
	for key in r.smembers('ess_Rivera_Snchez'):
		field_num = r.hlen(key) / 2
		for i in range(field_num):
			s_name = 'S' + str(i)
			if (r.hget(key, s_name) == 'ess_Rivera_Snchez'):
				p_name = 'P' + str(i)
				print(r.hget(key, p_name) + ', ' + key)
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

#给定一个oi, 给出它所有的S和P，<S, P,oi>
if query_type == 2:
	print('Type 2 with optimize:')
	time1 = time.time()
	field_num = r.hlen('Pueblo_of_Naranjito\r\n') / 2
	for i in range(field_num):
		s_name = 'S' + str(i)
		p_name = 'P' + str(i)
		print(r.hget('Pueblo_of_Naranjito\r\n', s_name) + ', ' + r.hget('Pueblo_of_Naranjito\r\n', p_name))
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

#给定两个p1,p2, 给出同时拥有它们的S，<S, p1, *>, <S, p2, *>
if query_type == 3:
	print('Type 3 with optimize:')
	time1 = time.time()
	for s in r.sinter('isLeaderOf', 'actedIn'):
		print(s)
		time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))

#给定一个oi, 给出拥有这样oi最多的S
if query_type == 4:
	print('Type 4 with optimize:')
	time1 = time.time()
	s_list = []
	key_name = 'Pueblo_of_Naranjito\r\n'
	field_num = r.hlen(key_name) / 2
	for i in range(field_num):
		s_name = 'S' + str(i)
		s = r.hget(key_name, s_name)
		if (s not in s_list):
			s_list.append({'name':s, 'cnt':1})
		else:
			index = s_list.index(s)
			s_list[index]['cnt'] += 1
	max_s = 0
	for i in range(len(s_list)):
		tmp = s_list[i]['cnt']
		if (tmp > max_s):
			max_s = tmp
	for s in s_list:
		if (s['cnt'] == max_s):
			print(s['name'] + ', ' + str(s['cnt']))
	time2 = time.time()
	print("Use %.7f seconds" % (time2 - time1))