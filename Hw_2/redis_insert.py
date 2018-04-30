#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import redis

#连接redis
r = redis.Redis(host = '127.0.0.1', port = 6379, db = 0)
if r.ping():
    print('Connect to Redis: Success')

#清空已有的数据库
r.flushall()

#读入yagoThreeSimplified.txt，将数据存入数据库，建立S上的索引
cnt = 0
with open('yagoThreeSimplified.txt', 'r') as f:
    for line in f:
        if (line[-3] != ' '):
            rdf = {}
            rdf['S'] = line.split(' ')[0]
            rdf['P'] = line.split(' ')[1]
            rdf['O'] = line.split(' ')[2]
            key_num = r.hlen(rdf['O']) / 2
            key_name = 'S' + str(key_num)
            status = r.hset(name = rdf['O'], key = key_name, value = rdf['S'])
            key_name = 'P' + str(key_num)
            status += r.hset(name = rdf['O'], key = key_name, value = rdf['P'])
            if (status != 2):
                print('Fail to Insert a Record: ' + rdf['S'] + ', ' + rdf['P'] + ', ' +rdf['O'])
            else:
                cnt += 1
            #建立S到O的索引和P到S的索引
            r.sadd(rdf['S'], rdf['O'])
            r.sadd(rdf['P'], rdf['S'])
print('Insert ' + str(cnt) + ' Records Totally')
