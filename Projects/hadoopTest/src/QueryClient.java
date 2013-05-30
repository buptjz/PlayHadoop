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
		client.userID = "8412672a";
		//client.traffic = "0";
		client.spName = "mop";
		client.run(); // 执行查询
	}

	public void run() {
		if (spName != null && traffic != null) { // 使用zoneID和traffic查询
			QueryBySpAndTraffic query;
			try {
				query = new QueryBySpAndTraffic(userID, fileName);
				query.start();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else if (spName != null) { // 仅使用zoneID查询
			QueryBySp query;
			try {
				query = new QueryBySp(spName, fileName);
				query.start();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

		} else if (userID != null) { // 仅使用traffic查询
			QueryByUserID query;
			try {
				query = new QueryByUserID(userID, fileName);
				query.start();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		if (userID != null) { // 仅使用traffic查询
			
		}
	}

}
