package com.zhaif;
/*
 * 实验代码 for blog：
 *     http://zhaif.us:8080/2017/09/17/python-hbase-tip-2/
 * 测试 scan filter, 对比 python/java
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) throws IOException {
		System.out.println("Hello World!");
        test_hbase_scan();
		System.out.println("=========================================");

	}


	public static void test_hbase_scan() throws IOException {
		// 问题 2 复现
		TableName tableName = TableName.valueOf("test_article_2");
		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(tableName);

		// scan python data
		// 写入的时候每秒 1 条，所以这段时间的数据为 10 条
		Scan s = new Scan(Bytes.toBytes("ARTICLE1.505024365e+15"), Bytes.toBytes("ARTICLE1.505024375e+15"));
		ResultScanner scanner = table.getScanner(s);
		int num = 0;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			num++;
		}
		System.out.println("Found row: " + num); // 预期 10，结果为 10

		// scan java data
		tableName = TableName.valueOf("test_article_java_1");
		table = conn.getTable(tableName);
		s = new Scan(Bytes.toBytes("ARTICLE1505031256242000"), Bytes.toBytes("ARTICLE1505031256259000"));
		scanner = table.getScanner(s);
		num = 0;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			num++;
		}
		System.out.println("Found row: " + num); // 预期 10，结果为 10

	}

}

