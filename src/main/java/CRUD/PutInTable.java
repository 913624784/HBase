package CRUD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutInTable {

	public static void main(String[] args) throws IOException {
		// 1，获取系统的资源情况
		// Configuration conf=HBaseConfiguration();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.170.133");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		// 2，管理者Admin
		// HBaseAdmin admin=new HBaseAdmin(conf);
		Connection conn = ConnectionFactory.createConnection(conf);// 获取连接
		Table tb = conn.getTable(TableName.valueOf("tbfilm"));
		//单条数据插入
		Put put = new Put(Bytes.toBytes("row3"));
		put.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("col2"), Bytes.toBytes("val1"));
		put.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("col1"), Bytes.toBytes("val2"));
		//tb.put(put);

		//多条数据插入
		List<Put> puts=new ArrayList<>();
		Put p = new Put(Bytes.toBytes("row-H002"));
        Put pp = new Put(Bytes.toBytes("row-S003"));
        Put ppp = new Put(Bytes.toBytes("row-S004"));
		p.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("col1"), Bytes.toBytes("1"));
		p.addColumn(Bytes.toBytes("fam6"), Bytes.toBytes("col2"), Bytes.toBytes("1"));
        pp.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("col1"), Bytes.toBytes("2"));
        pp.addColumn(Bytes.toBytes("fam6"), Bytes.toBytes("col2"), Bytes.toBytes("1"));
        ppp.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("col1"), Bytes.toBytes("1"));
        ppp.addColumn(Bytes.toBytes("fam6"), Bytes.toBytes("col2"), Bytes.toBytes("1"));
		puts.add(p);
		puts.add(pp);
		puts.add(ppp);
		tb.put(puts);

		tb.close();
		conn.close();



	}

}
