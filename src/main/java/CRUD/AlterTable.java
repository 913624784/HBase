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

public class AlterTable {

	public static void main(String[] args) throws IOException {
		// 1，获取系统的资源情况
		// Configuration conf=HBaseConfiguration();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.170.133");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		TableName name=TableName.valueOf("tablename");
		// 2，管理者Admin
		// HBaseAdmin admin=new HBaseAdmin(conf);
		Connection conn = ConnectionFactory.createConnection(conf);// 获取连接
		Admin admin = conn.getAdmin();
		HTableDescriptor htd=admin.getTableDescriptor(name);//这个是获得当前表 如果new一个 则会覆盖内容
		HColumnDescriptor hcd=new HColumnDescriptor(Bytes.toBytes("fam8"));
		//对列的修改
		hcd.setBlocksize(65536);
		//把列描述提交给表描述
		htd.addFamily(hcd);
		//修改列族
		//htd.modifyFamily(hcd);
		//先禁用表 再修改
		//admin.disableTable(name);//无论表状态是什么样 都能修改列族和表结构


		admin.modifyTable(name, htd);//定义表结构
		//admin.modifyColumn(name, hcd);//定义列族结构

		//admin.enableTable(name);
		//关闭资源 注意顺序
		admin.close();
		conn.close();

	}

}
