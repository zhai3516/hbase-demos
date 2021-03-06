#!/usr/bin/env python
# coding:utf-8
'''
实验代码 for blog：
    http://zhaif.us:8080/2017/09/17/python-hbase-tip-2/
测试 filter、scan 的一些小问题
'''
import struct
import time

import happybase

conn_pool = None
TABLE = 'article'


def get_connetion_pool(timeout=2):
    global conn_pool
    if conn_pool is None:
        conn_pool = happybase.ConnectionPool(1)
    return conn_pool


def recent_events_v1(start, end, table=None, filter_str=None, limit=2000):
    with get_connetion_pool().connection() as conn:
        if table is not None:
            t = conn.table(table)
        else:
            t = conn.table(TABLE)
        start_row = 'ARTICLE' + str(start * 1000000)
        end_row = 'ARTICLE' + str(end * 1000000)
        return t.scan(row_start=start_row, row_stop=end_row, filter=filter_str, limit=limit)


def recent_events_v2(start, end, table=None, filter_str=None, limit=2000):
    with get_connetion_pool().connection() as conn:
        if table is not None:
            t = conn.table(table)
        else:
            t = conn.table(TABLE)
        start_row = 'ARTICLE{time}'.format(time=struct.pack('>q', start * 1000000))
        end_row = 'ARTICLE{time}'.format(time=struct.pack('>q', end * 1000000))
        return t.scan(row_start=start_row, row_stop=end_row, filter=filter_str, limit=limit)


def save_batch_events(datas, table=None):
    with get_connetion_pool().connection() as conn:
        if table is not None:
            t = conn.table(table)
        else:
            t = conn.table(TABLE)
        b = t.batch(transaction=False)
        for row, data in datas.items():
            b.put(row, data)
        b.send()


def save_main_v2():
    datas = dict()
    for i in range(100):
        article_type_id = i % 2
        timestamp = time.time() + i
        rowkey = "ARTICLE" + str(timestamp * 1000000)
        data = {
            "basic:" + "ArticleID": str(i),
            "basic:" + "ArticleTypeID": struct.pack('>q', article_type_id),
            "basic:" + "Created": str(timestamp),
        }
        datas[rowkey] = data
    save_batch_events(datas, table="test_article_2")


def save_main_v3():
    datas = dict()
    for i in range(100):
        article_type_id = i % 2
        timestamp = time.time() + i
        rowkey = "ARTICLE" + struct.pack('>q', timestamp * 1000000)
        data = {
            "basic:" + "ArticleID": str(i),
            "basic:" + "ArticleTypeID": struct.pack('>q', article_type_id),
            "basic:" + "Created": str(timestamp),
        }
        datas[rowkey] = data
    save_batch_events(datas, table="test_article_3")

if __name__ == '__main__':
    save_main_v2()

    # 问题2重现
    results = recent_events_v1(start=0, end=1505024364, table="test_article_2")
    print len([i for i in results])  # 期望值为50, 实际值为100
    results = recent_events_v1(start=1505024365, end=1505024365 + 10, table="test_article_2")
    print len([i for i in results])  # 期望值为10, 实际值为0

    # 问题2修复
    results = recent_events_v1(start=0.0, end=1505024364.0, table="test_article_2")
    print len([i for i in results])  # 期望值为50, 实际值为50
    results = recent_events_v1(start=1505024365.0, end=1505024365.0 + 10, table="test_article_2")
    print len([i for i in results])  # 期望值为10, 实际值为10

    # 问题2思考
    save_main_v3()  # 导入100 条数据，50条ArticleTypeID=0，50条ArticleTypeID=1
    results = recent_events_v2(start=0, end=1505027700, table="test_article_3")
    print len([i for i in results])  # 期望值为50, 实际值为50
    results = recent_events_v2(start=1505027700, end=1505027700 + 10, table="test_article_3")
    print len([i for i in results])  # 期望值为10, 实际值为10

    print "Hello World!"
