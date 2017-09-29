package com.zhaif;
/*
 * 实验代码 for blog：
 *     http://zhaif.us:8080/2017/09/09/python-hbase-tip-1/
 *     http://zhaif.us:8080/2017/09/17/python-hbase-tip-2/
 * 测试 scan filter, 对比 python/java
 */
import java.io.IOException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) throws IOException {
		System.out.println("Hello World!");
        test_quote();
		System.out.println("=========================================");

	}

   	public static void test_quote() throws IOException {
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

