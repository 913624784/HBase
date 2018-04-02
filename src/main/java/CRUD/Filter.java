package CRUD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

import java.io.IOException;

public class Filter {

	public static void main(String[] args) throws IOException {
		// 1，获取系统的资源情况
		// Configuration conf=HBaseConfiguration();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.170.133");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		TableName name = TableName.valueOf("tbfilm");
		// 2，管理者Admin
		// HBaseAdmin admin=new HBaseAdmin(conf);
		Connection conn = ConnectionFactory.createConnection(conf);// 获取连接
		Admin admin = conn.getAdmin();

		Table tb = conn.getTable(name);
		CompareFilter filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new SubstringComparator("fam2"));

		Scan s1 = new Scan();
		s1.setFilter(filter1);
		ResultScanner rsn1 = tb.getScanner(s1);
		for (Result r : rsn1) {
			System.out.println(r);
		}
		System.out.println("======================");



		CompareFilter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new RegexStringComparator("row-S.*"));

		Scan s2 = new Scan();
		s2.setFilter(filter2);
		ResultScanner rsn2 = tb.getScanner(s2);
		for (Result r : rsn2) {
			System.out.println(r);
		}
		System.out.println("======================");



		CompareFilter filter3 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator("2"));

		Scan s3 = new Scan();
		s3.setFilter(filter3);
		ResultScanner rsn3 = tb.getScanner(s3);
		for (Result r : rsn3) {
			System.out.println(r);
		}

		tb.close();
		admin.close();
		conn.close();

	}

}
