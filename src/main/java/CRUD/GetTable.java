package CRUD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetTable {

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

		Table tb=conn.getTable(name);
		Get get=new Get(Bytes.toBytes("row-H002"));
		get.getRow();
		Result rs=tb.get(get);
		get.setMaxVersions();
		for(Cell cell:rs.rawCells()){
			System.out.println(cell);
			/*row-H002/fam2:col1/1520693546881/Put/vlen=1/seqid=0
			row-H002/fam3:col2/1520693546881/Put/vlen=1/seqid=0*/
		}
		//keyvalues={row-H002/fam2:col1/1520693546881/Put/vlen=1/seqid=0, row-H002/fam3:col2/1520693546881/Put/vlen=1/seqid=0}
		  System.out.println(rs);
		for(Cell cell:rs.rawCells()){
			System.out.print(new String(CellUtil.cloneRow(cell)));
			System.out.print(new String(CellUtil.cloneFamily(cell)));
			System.out.print(new String(CellUtil.cloneQualifier(cell)));
			System.out.println(new String(CellUtil.cloneValue(cell)));
		}

		tb.close();
		conn.close();







	}

}
