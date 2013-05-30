/**
 * HBase（3）作业
 * 通过Sp（非Rowkey）的方式进行查询
 * **/

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class QueryBySp extends Thread {

	private String spName;
	private FileOutputStream output;
	private HTableInterface sptable; // 表对象
	private HTableInterface table; // 表对象
	HTablePool tablePool;

	public QueryBySp(String spName, String fileName) throws FileNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		tablePool = new HTablePool(conf, 1); // 创建表连接池
		this.sptable = tablePool.getTable("acT_sp1"); // sp索引表
		this.table = tablePool.getTable("acT1"); // 主表
		this.spName = spName; // 用户ID查询条件
		this.output = new FileOutputStream(fileName); // 结果文件
		//start(); // 启动线程
	}

	public void dataWithGetList(List<Get> getList) throws IOException{
		if (!getList.isEmpty()) { // 执行剩余的get
			Result[] results = table.get(getList);
			for (Result r : results) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getValue(),"UTF-8"));
					System.out.print('\t');
				}
				System.out.println();
			}
			table.close();
		}
	}

	public void run() {
		Scan scan = new Scan(); // scan对象
		scan.addColumn("index".getBytes(), "userID".getBytes()); // 列
		scan.setCaching(500); // scan缓存
		scan.setCacheBlocks(false); // 不缓存块
		scan.setStartRow(Bytes.toBytes(spName + ",")); // 起始rowkey
		scan.setStopRow(Bytes.toBytes(spName + ",9")); // 结束rowkey
		List<Get> getList = new ArrayList<Get>();

		try {
			ResultScanner rscaner = sptable.getScanner(scan);
			sptable.close();
			Result rs = rscaner.next();
			if(rs==null){
				System.out.println("no record");
			}else
			{
				while(rs!=null){
					List<KeyValue> kvList = rs.list();
					for(KeyValue kv : kvList){
						byte[] index = kv.getValue();
						Get get = new Get(index);
						getList.add(get); // 放入get列表
					}
					rs = rscaner.next();
				}
				dataWithGetList(getList);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
