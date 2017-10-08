#!/usr/bin/env python
# coding:utf-8
'''
实验代码 for blog：
    http://zhaif.us:8080/2017/09/09/python-hbase-tip-1/
    http://zhaif.us:8080/2017/09/17/python-hbase-tip-2/
    http://zhaif.us:8080/2017/09/28/python-hbase-tip-3/
    http://zhaif.us:8080/2017/10/08/python-hbase-tip-4/
测试 filter、scan 的一些小问题
'''
import socket
import struct
import time
import traceback

import happybase

conn_pool = None
TABLE = 'article'


def get_connetion_pool(timeout=2):
    global conn_pool
    if conn_pool is None:
        conn_pool = happybase.ConnectionPool(1, timeout=timeout)
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


def recent_events_v3(start, end, table=None, filter_str=None, limit=2000, timeout=2):
    # 为了复现错误，通过 pool 那 conn 时重新设置 timeout
    with get_connetion_pool().connection(timeout) as conn:
        if table is not None:
            t = conn.table(table)
        else:
            t = conn.table(TABLE)
        start_row = 'ARTICLE' + str(start * 1000000)
        end_row = 'ARTICLE' + str(end * 1000000)
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


def save_main_v1():
    datas = dict()
    for i in range(100):
        article_type_id = i % 2
        timestamp = time.time() + i
        rowkey = "ARTICLE" + str(timestamp * 1000000)
        data = {
            "basic:" + "ArticleID": str(i),
            "basic:" + "ArticleTypeID": str(article_type_id),
            "basic:" + "Created": str(timestamp),
        }
        datas[rowkey] = data
    save_batch_events(datas, table="test_article_1")


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
    # 问题1重现
    # save_main_v1()  # 导入100 条数据，50条ArticleTypeID=0，50条ArticleTypeID=1
    #filter_str = "SingleColumnValueFilter('basic', 'ArticleTypeID', =, 'binary:1')"
    #results = recent_events_v1(start=0, end=1505023900, table="test_article_1", filter_str=filter_str)
    # print len([i for i in results])  # 期望值为50, 实际值为50, 但是使用java 查询确是0

    # 问题1修复
    # save_main_v2()  # 导入100 条数据，50条ArticleTypeID=0，50条ArticleTypeID=1
    # filter_str = "SingleColumnValueFilter('basic', 'ArticleTypeID', =, 'binary:{value}')".format(value=struct.pack('>q', 1))
    # results = recent_events_v1(start=0, end=1505023900, table="test_article_2", filter_str=filter_str)
    # print len([i for i in results])  # 期望值为50, 实际值为50，使用java 查询也是50

    # 问题2重现
    # results = recent_events_v1(start=0, end=1505024364, table="test_article_2")
    # print len([i for i in results])  # 期望值为50, 实际值为100
    # results = recent_events_v1(start=1505024365, end=1505024365 + 10, table="test_article_2")
    # print len([i for i in results])  # 期望值为10, 实际值为0

    # 问题2修复
    # results = recent_events_v1(start=0.0, end=1505024364.0, table="test_article_2")
    # print len([i for i in results])  # 期望值为50, 实际值为50
    # results = recent_events_v1(start=1505024365.0, end=1505024365.0 + 10, table="test_article_2")
    # print len([i for i in results])  # 期望值为10, 实际值为10

    # 问题2思考
    # save_main_v3()  # 导入100 条数据，50条ArticleTypeID=0，50条ArticleTypeID=1
    # results = recent_events_v2(start=0, end=1505027700, table="test_article_3")
    # print len([i for i in results])  # 期望值为50, 实际值为50
    # results = recent_events_v2(start=1505027700, end=1505027700 + 10, table="test_article_3")
    # print len([i for i in results])  # 期望值为10, 实际值为10

    # 问题3复现
    # for i in range(5):
    #     try:
    #         filter_str = "SingleColumnValueFilter('basic', 'ArticleTypeID', =, 'binary:{value}')".format(value=struct.pack('>q', 52909257))
    #         results = recent_events_v1(start=0, end=1505646570, table="test_article_java_2", filter_str=filter_str if i % 2 == 0 else None)
    #         print len([i for i in results])  # 期望值为2, 实际报错
    #     except:
    #         print traceback.format_exc()
    #     time.sleep(5)
    #     print '######################################################'

    # 问题3修复
    # filter_str = "SingleColumnValueFilter('basic', 'ArticleTypeID', =, 'binary:{value}')".format(value=struct.pack('>q', 52909257).replace("'", "''"))
    # results = recent_events_v1(start=0, end=1505646570, table="test_article_java_2")
    # print len([i for i in results])  # 期望值为3 , 实际结果为3
    # results = recent_events_v1(start=0, end=1505646570, table="test_article_java_2", filter_str=filter_str)
    # print len([i for i in results])  # 期望值为2 , 实际结果为2

    # 问题4复现
    # for i in range(10):
    #     try:
    #         results = recent_events_v3(start=0, end=1505646570, table="test_article_java_2", timeout=0.1)  # 把timeout 设置的非常小观察错误出现
    #         print len([i for i in results])  # 期望值为2, 实际报错
    #     except Exception as e:
    #         #print traceback.format_exc()
    #         print e
    #     time.sleep(2)
    #     print '######################################################'

    # 问题4修复
    # for i in range(10):
    #    try:
    #        results = recent_events_v3(start=0, end=1505646570, table="test_article_java_2", timeout=0.01)  # 把timeout 设置的非常小观察错误出现
    #        print len([i for i in results])  # 期望值为2, 实际报错
    #    except socket.timeout:
    #        conn_pool = None  # catch timeout 后, 清空连接池，下次使用时重新初始化, 仅限单线程模型 !
    #        print 'time out: reinit conn pool!'
    #    # 不会在出现 `TApplicationException: Missing result` 错误
    #    time.sleep(2)
    #    print '######################################################'

    print "Hello World!"
