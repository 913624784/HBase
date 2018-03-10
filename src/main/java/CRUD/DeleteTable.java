package CRUD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class DeleteTable {

	public static void main(String[] args) throws IOException {
		// 1，获取系统的资源情况
		// Configuration conf=HBaseConfiguration();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.170.133");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		TableName name = TableName.valueOf("t1");
		// 2，管理者Admin
		// HBaseAdmin admin=new HBaseAdmin(conf);
		Connection conn = ConnectionFactory.createConnection(conf);// 获取连接
		Admin admin = conn.getAdmin();
		boolean b=admin.tableExists(name);
		if(b){
			if(admin.isTableEnabled(name)){
				admin.disableTable(name);
			}
			admin.deleteTables("t1");
		}
		admin.close();
		conn.close();

	}

}
