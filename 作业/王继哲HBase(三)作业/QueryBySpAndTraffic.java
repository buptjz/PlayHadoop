/**
 * HBase(3)作业
 * 非rowkey的联合查询
 * **/
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class QueryBySpAndTraffic extends Thread {
	private String spName;
	private String traffic;
	private FileOutputStream output;
	private HTableInterface table; // 表对象
	private HTableInterface spTable; // 表对象
	private HTableInterface traTable; // 表对象
	HTablePool tablePool;

	public QueryBySpAndTraffic(String spName,String traffic, String fileName) throws FileNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		tablePool = new HTablePool(conf, 1); // 创建表连接池
		this.table = tablePool.getTable("acT1"); // 开启表
		this.spTable = tablePool.getTable("acT_sp1"); // 开启表
		this.traTable = tablePool.getTable("acT_up1"); // 开启表
		this.output = new FileOutputStream(fileName); // 结果文件
		this.spName = spName;
		this.traffic = traffic;
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
	
	public List<Get> mergeIndexForGet(ResultScanner traRS, ResultScanner spRS) throws UnsupportedEncodingException {
		List<Get> getList = new ArrayList<Get>();
		BloomFilter bf = new BloomFilter(300000, 100000, 3000);
		for (Result r : traRS) { // 将traffic索引表检索结果放入bloom filter中
			for (KeyValue kv : r.raw()) {
				bf.put(new String(kv.getValue(),"UTF-8"));
//				System.out.print(new String(kv.getValue(),"UTF-8"));
//				System.out.println("traffic");
			}
		}
		for (Result r : spRS) { // 对sp索引表检索结果进行过滤
			for (KeyValue kv : r.raw()) {
				byte[] index = kv.getValue();
//				System.out.print(new String(kv.getValue(),"UTF-8"));
//				System.out.println("sp");
				if (bf.contains(new String(index,"UTF-8"))) { // 在小区索引表中存在的,构造get请求
					Get get = new Get(index);
					getList.add(get);
				}
			}
		}
		return getList;
	}

	public void run() {
		//创建sp搜索
		Scan scanSp = new Scan(); // scan对象
		scanSp.addColumn("index".getBytes(), "userID".getBytes()); // 列
		scanSp.setCaching(500); // scan缓存
		scanSp.setCacheBlocks(false); // 不缓存块
		scanSp.setStartRow(Bytes.toBytes(spName + ",")); // 起始rowkey
		scanSp.setStopRow(Bytes.toBytes(spName + ",9")); // 结束rowkey
		
		//创建traffic搜索
		Scan scanTraffic = new Scan();
		scanTraffic.addColumn("index".getBytes(), "userID".getBytes()); 
		scanTraffic.setCaching(500); 
		scanTraffic.setCacheBlocks(false); 
		scanTraffic.setStartRow(Bytes.toBytes(traffic + ","));
		scanTraffic.setStopRow(Bytes.toBytes(traffic + ",9"));
		
		try {
			//搜索
			ResultScanner traScaner = traTable.getScanner(scanTraffic);
			ResultScanner spScaner = spTable.getScanner(scanSp);
			traTable.close();
			spTable.close();
			//利用BloomFilter合并搜索结果
			List<Get> gets = mergeIndexForGet(traScaner,spScaner);
			//利用合并的结果进行查询
			dataWithGetList(gets);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
