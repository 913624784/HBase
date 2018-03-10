package CRUD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CreateTable {

	public static void main(String[] args) throws IOException {
		//1，获取系统的资源情况
		//Configuration conf=HBaseConfiguration();
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.170.133");
		conf.set("hbase.zookeeper.property.clientPort","2181");

		//2，管理者Admin
		//HBaseAdmin admin=new HBaseAdmin(conf);
		Connection conn=ConnectionFactory.createConnection(conf);//获取连接
		Admin admin=conn.getAdmin();
		//3，表达描述：负责把表的结构给绘制出来
		//HTableDescriptor desc=new HTableDescriptor(Bytes.toBytes("tablename")) ;
		HTableDescriptor desc=new HTableDescriptor(TableName.valueOf("tbfilm")) ;
		HColumnDescriptor hcd=new HColumnDescriptor(Bytes.toBytes("fam1"));
		HColumnDescriptor hcd1=new HColumnDescriptor(Bytes.toBytes("fam2"));
		HColumnDescriptor hcd2=new HColumnDescriptor(Bytes.toBytes("fam6"));
		desc.addFamily(hcd);
		desc.addFamily(hcd1);
		desc.addFamily(hcd2);

		//4，把描述好的表给admin admin把表建出来
		admin.createTable(desc);
		//5，资源关闭 注意关闭顺序
		admin.close();
		conn.close();

	}

}
