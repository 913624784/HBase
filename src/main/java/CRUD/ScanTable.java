package CRUD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class ScanTable {

	public static void main(String[] args) throws IOException {
		// 1，获取系统的资源情况
		// Configuration conf=HBaseConfiguration();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.170.133");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		// 2，管理者Admin
		// HBaseAdmin admin=new HBaseAdmin(conf);
		TableName name = TableName.valueOf("tbfilm");
		Connection conn = ConnectionFactory.createConnection(conf);// 获取连接
		Admin admin = conn.getAdmin();
		//获取单个表的描述
		/*HTableDescriptor htd = admin.getTableDescriptor(name);
		System.out.println(htd);*/
		//获取所有表的描述
		HTableDescriptor htd[]=admin.listTables();
		Table tb=conn.getTable(name);
		for(HTableDescriptor h:htd){
			System.out.println(h);
		}
		Scan s=new Scan();
		/*s.setStartRow(Bytes.toBytes("row2"));
		s.setStopRow(Bytes.toBytes("row4"));*/
		ResultScanner rsn=tb.getScanner(s);
		for(Result r:rsn){
			System.out.println(r);
		}
		admin.close();
		conn.close();


	}

}
