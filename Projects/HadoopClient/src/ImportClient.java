/**
 * HBase（3）作业
 * 多线程导入数据
 * **/
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

public class ImportClient {

	static HTablePool tablePool;
	public static Configuration conf = HBaseConfiguration.create();
	static String tableName = "acT1"; // 写入的表名
	static String SpNametableName = "acT_sp1"; 
	static String uptableName = "acT_up1"; 
	HTableInterface table = null; // 表操作对象
	HTablePool pool = null; // 表连接线程池

	public static void main(String[] args) throws Exception {
		if (args.length < 4) { // 程序使用方法提示
			System.out.println("Usage: inputPath threadNumber poolSize startIndex endIndex");
			System.exit(2);
		}
		File path = new File(args[0]); // 输入数据文件所在路径
		File[] fileList = path.listFiles(); // 输入数据文件列表
		int threadNumber = Integer.parseInt(args[1]); // 导入线程数1
		int poolSize = Integer.parseInt(args[2]); // 数据表连接池大小1
		int startIndex = Integer.parseInt(args[3]); // 起始文件索引0
		int endIndex = Integer.parseInt(args[4]); // 结束文件索引1
		int fileNumber = endIndex- startIndex; // 文件数量
		tablePool = new HTablePool(conf, poolSize); // 创建表连接池
		// 如果数据表不存在则创建
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		//创建主表
		if (!admin.tableExists(tableName)) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("content"));
			admin.createTable(tableDescriptor);
		}
		//创建索引表1
		if (!admin.tableExists(SpNametableName)) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(SpNametableName);
			tableDescriptor.addFamily(new HColumnDescriptor("index"));
			admin.createTable(tableDescriptor);
		}
		//创建索引表2
		if (!admin.tableExists(uptableName)) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(uptableName);
			tableDescriptor.addFamily(new HColumnDescriptor("index"));
			admin.createTable(tableDescriptor);
		}
		admin.close();
		
		Thread[] threadPool = new Thread[threadNumber]; // 初始化线程池
		int filesToBeRead = fileList.length - startIndex; // 剩余需要读取的文件数量
		while (fileNumber > 0 && filesToBeRead > 0) {
			if (filesToBeRead < threadNumber) { // 剩余文件数量小于线程数
				for (int i = 0; i < filesToBeRead; i++) { // 为每个文件创建一个导入线程
					threadPool[i] = new HBaseImportThread(i, fileList[startIndex + i].getCanonicalPath(),
							tablePool);
				}
				for (int i = 0; i < filesToBeRead; i++) {

					threadPool[i].join(); // 等待子线程结束后后续代码方可继续执行
				}
			} else { // 剩余文件数量大于等于线程数
				for (int i = 0; i < threadNumber; i++) { // 为每个文件创建一个导入线程
					threadPool[i] = new HBaseImportThread(i, fileList[startIndex + i].getCanonicalPath(),
							tablePool);
				}
				for (int i = 0; i < threadNumber; i++) {
					threadPool[i].join(); // 等待子线程结束后后续代码方可继续执行
				}
				startIndex += threadNumber; // 更新起始文件索引
				fileNumber -= threadNumber; // 更新总文件数量
				filesToBeRead -= threadNumber; // 更新剩余文件数量
			}
			tablePool.close(); // 关闭连接池
		}
	}
}