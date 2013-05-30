/**
 * Hadoop HBase（2）作业
 * 将access.logD文件写入HBase中
 * 没有使用Map-Reduce，也没有使用多线程，垃圾版
 * **/
import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

public class WriteHBase {

	public static ArrayList<String> readFileByLines(String fileName) {
		ArrayList<String> arrList = new ArrayList<String>();

		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				// 显示行号
				// System.out.println("line " + line + ": " + tempString);
				line++;
				arrList.add(tempString);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return arrList;
	}

	public static void createTable(ArrayList<String> stringList)
			throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		// set Table info
		HTableDescriptor tableDesc = new HTableDescriptor("LLTJ".getBytes());

		// set 列族 info
		HColumnDescriptor colDesc1 = new HColumnDescriptor(
				"DataInfo".getBytes());
		HColumnDescriptor colDesc2 = new HColumnDescriptor(
				"UserInfo".getBytes());
		HColumnDescriptor colDesc3 = new HColumnDescriptor(
				"ServerInfo".getBytes());

		// 将列族信息加入到表信息中
		tableDesc.addFamily(colDesc1);
		tableDesc.addFamily(colDesc2);
		tableDesc.addFamily(colDesc3);

		admin.createTable(tableDesc); // 创建表

		// 验证表是否创建成功
		Boolean isAvailable = admin.isTableAvailable("LLTJ".getBytes());
		System.out.println("Table student available: " + isAvailable);

		// 添加数据
		HTable table = new HTable(conf, "LLTJ");

		int lineNumber = 0;
		for (String c : stringList) {

			Put put = new Put(("row"+lineNumber).getBytes()); // 指定行索引

			put.add("DataInfo".getBytes(), "time".getBytes(), (c.split("\\s")[0]+c.split("\\s")[1]).getBytes());
			put.add("DataInfo".getBytes(), "up".getBytes(), c.split("\\s")[6].getBytes());
			put.add("DataInfo".getBytes(), "down".getBytes(), c.split("\\s")[7].getBytes());
			put.add("UserInfo".getBytes(), "userID".getBytes(), c.split("\\s")[2].getBytes());
			put.add("ServerInfo".getBytes(), "serverIP".getBytes(), c.split("\\s")[3].getBytes());
			put.add("ServerInfo".getBytes(), "hostName".getBytes(), c.split("\\s")[4].getBytes());
			put.add("ServerInfo".getBytes(), "spName".getBytes(), c.split("\\s")[5].getBytes());
			// DataInfo UserInfo SeverInfo SeverInfo SeverInfo DataInfo DataInfo
			// 0 1 2 3 4 5 6
			// time userID serverIP hostName spName uploadTraffic downloadTraffic
            //2012-07-05 00:06:20	319b7db6	60.28.212.62	hdn.xnimg.cn:80	xnimg	1065	18816

			table.put(put);
			lineNumber++;
		}
		admin.close();
	}

	public static void main(String[] args) throws Exception {
		// 创建HBaseAdmin对象
		ArrayList<String> stringList;
		stringList = readFileByLines("/home/wangjz/Desktop/access.logD");
		// for(String c : stringList){
		// System.out.println(c);
		// }
		createTable(stringList);
	}
}