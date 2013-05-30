/**
 * HBase（3）作业
 * 通过UserID(Rowkey)的方式进行查询
 * **/

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class QueryByUserID extends Thread {

	private String userID;
	private FileOutputStream output;
	private HTableInterface table; // 表对象
	HTablePool tablePool;

	public QueryByUserID(String userID, String fileName) throws FileNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		tablePool = new HTablePool(conf, 1); // 创建表连接池
		this.table = tablePool.getTable("accessTable1"); // 开启表
		this.userID = userID; // 用户ID查询条件
		this.output = new FileOutputStream(fileName); // 结果文件
		//start(); // 启动线程
	}

	public void run() {
		Scan scan = new Scan(); // scan对象
		//scan.addColumn("content".getBytes(), "userID".getBytes()); // 列
		scan.addFamily("content".getBytes());
		scan.setCaching(500); // scan缓存
		scan.setCacheBlocks(false); // 不缓存块
		scan.setStartRow(Bytes.toBytes(userID+",")); // 起始rowkey
		scan.setStopRow(Bytes.toBytes(userID + ",9")); // 结束rowkey
		try {
			ResultScanner rscaner = table.getScanner(scan);
			table.close();
			Result rs = rscaner.next();
			if(rs==null){
				System.out.println("no record");
			}else{
				while(rs!=null){
					List<KeyValue> kvList = rs.list();
					for(KeyValue kv : kvList){
						//System.out.print(new String(kv.getRow(),"UTF-8"));
						//System.out.println(new String(kv.getQualifier (),"UTF-8"));
						System.out.print(new String(kv.getValue(),"UTF-8"));
						System.out.print('\t');
					}
					System.out.println();
					rs = rscaner.next();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
