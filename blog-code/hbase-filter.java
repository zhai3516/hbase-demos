package com.zhaif;
/*
 * 实验代码 for blog：
 *     http://zhaif.us:8080/2017/09/09/python-hbase-tip-1/
 * 测试 java filter, 对比 python
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
		System.out.println("=========================================");

	}

	public static void test_hbase_filter() throws IOException {
		TableName tableName = TableName.valueOf("test_article_1");
		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(tableName);

		// Scan python table `test_article_1`
		System.out.println("Prepare to scan !");
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("basic"),
				Bytes.toBytes("ArticleTypeID"), CompareOp.EQUAL, Bytes.toBytes(1L));
		list.addFilter(filter1);
		Scan s = new Scan();
		s.addFamily(Bytes.toBytes("basic"));
		s.setFilter(list);
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
			Put p = new Put(Bytes.toBytes("ARTICLE" + (System.currentTimeMillis() + 1000) * 1000));
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

	}
}

