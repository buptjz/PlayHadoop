import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

//import org.apache.hadoop.util.bloom.BloomFilter;

public class QueryBySpAndTraffic extends Thread {

	private String userID;
	private FileOutputStream output;
	private HTableInterface table; // 表对象
	HTablePool tablePool;
	static final String BLOOM_FILTER_META_KEY = "BLOOM_FILTER_META";

	public List<Get> mergeIndexForGet(ResultScanner rsZone, ResultScanner rsTraffic) {
		List<Get> getList = new ArrayList<Get>();
		BloomFilter bf = new BloomFilter();
		for (Result r : rsZone) { // 将小区索引表检索结果放入bloom filter中
			for (KeyValue kv : r.raw()) {
				bf.add(kv.getValue());
			}
		}
		for (Result r : rsTraffic) { // 对流量索引表检索结果进行过滤
			for (KeyValue kv : r.raw()) {
				byte[] index = kv.getValue();
				if (bf.contains(index)) { // 在小区索引表中存在的,构造get请求
					Get get = new Get(index);
					get.addColumn("cf".getBytes(), "content".getBytes());
					getList.add(get);
				}
			}
		}
		return getList;
	}


	public QueryBySpAndTraffic(String userID, String fileName) throws FileNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		tablePool = new HTablePool(conf, 1); // 创建表连接池
		this.table = tablePool.getTable("accessTable1"); // 开启表
		this.userID = userID; // 用户ID查询条件
		this.output = new FileOutputStream(fileName); // 结果文件
		//start(); // 启动线程
	}

	public void run() {
		Scan scan = new Scan(); // scan对象
		scan.addColumn("content".getBytes(), "userID".getBytes()); // 列
		scan.addColumn("content".getBytes(), "up".getBytes()); // 列
		scan.setCaching(500); // scan缓存
		scan.setCacheBlocks(false); // 不缓存块
		scan.setStartRow(Bytes.toBytes(userID)); // 起始rowkey
		//scan.setStopRow(Bytes.toBytes(userID + "9")); // 结束rowkey
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
						System.out.println(new String(kv.getRow(),"UTF-8"));
						//System.out.println(new String(kv.getQualifier (),"UTF-8"));
						System.out.println(new String(kv.getValue(),"UTF-8"));
					}
					rs = rscaner.next();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
