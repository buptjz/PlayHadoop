import java.io.*;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseImportThread extends Thread{
	//private LogReader reader; // 读取数据文件类
	private BufferedReader reader;
	private HTableInterface table; // 表对象
	private HTableInterface spTable; // 表对象
	private HTableInterface upTable; // 表对象
	private int threadIndex; // 线程索引
	private HTablePool tablePool; // 数据库连接池
	private String tempString;
	public HBaseImportThread(int threadIndex, String fileName, HTablePool tablePool) {

		try {
			System.out.println("Thread index is " + threadIndex + ". File name is " + fileName);
			//this.reader = new LogReader(fileName);
			File file = new File(fileName);
			reader = new BufferedReader(new FileReader(file));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.threadIndex = threadIndex; // 保存线程索引
		this.tablePool = tablePool; // 保存表连接池
		start(); // 启动线程
	}

	public void run() {
		table = tablePool.getTable("acT1"); // 开启表
		spTable = tablePool.getTable("acT_sp1"); // 开启表
		upTable = tablePool.getTable("acT_up1"); // 开启表
		table.setAutoFlush(false); // 禁用自动提交
		spTable.setAutoFlush(false); // 禁用自动提交
		upTable.setAutoFlush(false); // 禁用自动提交

		try {
			while ((tempString = reader.readLine()) != null) { // 逐行读取数据文件(tempString = reader.readLine()) != null
				String[] columnList;
				columnList = tempString.split("\\s");
				Put put = new Put((columnList[2]+','+columnList[0]+columnList[1]).getBytes()); // userID+time当作rowkey
				put.add("content".getBytes(), "time".getBytes(), (columnList[0]+columnList[1]).getBytes());
				put.add("content".getBytes(), "userID".getBytes(), columnList[2].getBytes());
				put.add("content".getBytes(), "serverIP".getBytes(), columnList[3].getBytes());
				put.add("content".getBytes(), "hostName".getBytes(), columnList[4].getBytes());
				put.add("content".getBytes(), "spName".getBytes(), columnList[5].getBytes());
				put.add("content".getBytes(), "up".getBytes(), columnList[6].getBytes());
				put.add("content".getBytes(), "down".getBytes(), columnList[7].getBytes());
				table.put(put);

				Put putsp = new Put((columnList[5]+','+columnList[0]+columnList[1]).getBytes()); // spName+time当作rowkey
				putsp.add("index".getBytes(), "userID".getBytes(), (columnList[2]+','+columnList[0]+columnList[1]).getBytes());
				spTable.put(putsp);

				Put putup = new Put((columnList[6]+','+columnList[0]+columnList[1]).getBytes()); // up+time当作rowkey
				putup.add("index".getBytes(), "userID".getBytes(), (columnList[2]+','+columnList[0]+columnList[1]).getBytes());
				upTable.put(putup);

			}
			table.flushCommits();
			spTable.flushCommits();
			upTable.flushCommits();
			table.close();
			spTable.close();
			upTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
