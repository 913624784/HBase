package CRUD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteColumn {

	public static void main(String[] args) throws IOException {
		// 1，获取系统的资源情况
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.170.133");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		TableName name = TableName.valueOf("tablename");
		// 2，管理者Admin
		Connection conn = ConnectionFactory.createConnection(conf);// 获取连接
		Admin admin = conn.getAdmin();
		//admin.deleteColumn(name, Bytes.toBytes("fam8"));//列族属于表结构 要用admin删除
		Table tb=conn.getTable(name);
		Delete d=new Delete(Bytes.toBytes("row7"));
		tb.delete(d);//用tb删除row

		tb.close();
		conn.close();

	}

}
