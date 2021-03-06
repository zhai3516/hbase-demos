package com.zhaif;
/*
 * 实验代码 for blog：
 *     http://zhaif.us:8080/2017/09/09/python-hbase-tip-1/
 *     http://zhaif.us:8080/2017/09/17/python-hbase-tip-2/
 *     http://zhaif.us:8080/2017/09/28/python-hbase-tip-3/
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
		test_hbase_filter();
        test_hbase_scan();
        test_quote();
		System.out.println("=========================================");

	}

    public static void test_hbase_filter() throws IOException {
		// 问题 1 复现
		TableName tableName = TableName.valueOf("test_article_1");
		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(tableName);

		// 创建 filter list
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("basic"),
				Bytes.toBytes("ArticleTypeID"), CompareOp.EQUAL, Bytes.toBytes(1L));
		list.addFilter(filter1);
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes("basic"));
		s.setFilter(list);

		// Scan python table `test_article_1`
		System.out.println("Prepare to scan !");
		ResultScanner scanner = table.getScanner(s);
		int num = 0;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			num++;
		}
		System.out.println("Found row: " + num);// 预期 50，结果为 0

		// create test_article_java_1
		tableName = TableName.valueOf("test_article_java_1");
		table = conn.getTable(tableName);
		System.out.println("Prepare create table !");
		Admin admin = conn.getAdmin();
		if (!admin.tableExists(tableName)) {
			HTableDescriptor td = new HTableDescriptor(tableName);
			HColumnDescriptor basic = new HColumnDescriptor("basic");
			td.addFamily(basic);
			admin.createTable(td);
			System.out.println("Created !");
		}

		// Put value to test_article_java_1
		System.out.println("Prepare to write data to: " + table.getName().toString());
		for (int i = 0; i < 100; i++) {
			Put p = new Put(Bytes.toBytes("ARTICLE" + (System.currentTimeMillis()) * 1000));
			p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("ArticleTypeID"), Bytes.toBytes(Long.valueOf(i % 2)));
			table.put(p);
		}

		// scan test_article_java_1
		scanner = table.getScanner(s);
		num = 0;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			num++;
		}
		System.out.println("Found row: " + num);// 预期 50，结果为 50

		// scan test_article_2 (用python写入的二级制数据表)
		tableName = TableName.valueOf("test_article_2");
		table = conn.getTable(tableName);
		scanner = table.getScanner(s);
		num = 0;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			num++;
		}
		System.out.println("Found row: " + num);// 预期 50，结果为 50

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

   	public static void test_quote() throws IOException {
        // 问题 3 复现
		TableName tableName = TableName.valueOf("test_article_java_2");
		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(tableName);

		// Put value to test_article_java_2
		System.out.println("Prepare to write data to: " + table.getName().toString());
		Put p = new Put(Bytes.toBytes("ARTICLE" + (System.currentTimeMillis()) * 1000));
		p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("ArticleTypeID"), Bytes.toBytes(Long.valueOf(52909257)));
		table.put(p);
		p = new Put(Bytes.toBytes("ARTICLE" + (System.currentTimeMillis()) * 1000));
		p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("ArticleTypeID"), Bytes.toBytes(Long.valueOf(12345678)));
		table.put(p);

		// Prepare filters
		System.out.println("Prepare to scan !");
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("basic"),
				Bytes.toBytes("ArticleTypeID"), CompareOp.EQUAL, Bytes.toBytes(Long.valueOf(52909257)));
		list.addFilter(filter1);

		// scan with filter
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes("basic"));
		s.setFilter(list);
		ResultScanner scanner = table.getScanner(s);
		int num = 0;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			num++;
			System.out.println(rr.toString());
		}

		System.out.println("Found row: " + num); // 预期 1，结果 1
	}
}

