/**
 * HBase(3)作业
 * 查询的Main函数
 * 支持三种查询方式：1）rowkey	2）非Rowkey 3)非Rowkey联合查询
 * **/
import java.io.FileNotFoundException;
import java.io.IOException;

public class QueryClient extends Thread{

	private String fileName;
	private String userID;
	private String traffic;
	private String spName;

	public static void main(String[] args) throws IOException, InterruptedException {
		QueryClient client = new QueryClient();
		client.fileName = "/home/wangjz/Development/PlayHadoop/data/test/result.log"; // 结果文件路径
		
		//在此处设置查询条件：
		//client.userID = "56932c62";
		//client.traffic = "0";
		client.spName = "sinaimg";
		client.run(); // 执行查询
	}

	public void run() {
		if (spName != null && traffic != null) { // 使用spName和traffic查询
			QueryBySpAndTraffic query;
			try {
				query = new QueryBySpAndTraffic(spName, traffic,fileName);
				query.start();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else if (spName != null) { // 仅使用spName查询
			QueryBySp query;
			try {
				query = new QueryBySp(spName, fileName);
				query.start();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

		} else if (userID != null) { // 仅使用userID查询
			QueryByUserID query;
			try {
				query = new QueryByUserID(userID, fileName);
				query.start();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}
