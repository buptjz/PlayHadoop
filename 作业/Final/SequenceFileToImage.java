/**
 * 能够将SeqFile转换成单独的Image并调用OpencvTest程序来计算图像的特征
 * **/
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
public class SequenceFileToImage {
	
	//运行本地函数的程序
	public static String RunProcess(String[] args){
		int exitcode = -1;
		String outputValue = new String(); 
		try   
		{               
			Runtime runtime = Runtime.getRuntime();   
			final Process process=runtime.exec(args);
			InputStream input = process.getInputStream();   
			InputStreamReader isr = new InputStreamReader(input);   
			BufferedReader br = new BufferedReader(isr);   
			String line = null;
			while ( (line = br.readLine()) != null)   {
				//System.out.println(line);   
				outputValue += line;
			}
			int exitVal = process.waitFor();   
			//System.out.println("Process exitValue: " + exitVal);   
		} catch (Throwable t){   
			t.printStackTrace();   
		}   
		return outputValue;
	}
	
	public static class SeqToImageMapper extends Mapper<Text, BytesWritable, Text, Text>{
		private String exePath;
		public void map(Text key, BytesWritable value, Context context) throws IOException,InterruptedException {
	        //将文件保存在本地目录中
//			System.out.println(key.toString());
	        String fileName = "/home/wangjz/Desktop/localImages/"+key.toString();//.split("/")[6];
	        DataOutputStream out=new DataOutputStream(new FileOutputStream(fileName));
//	        System.out.print("outImageLength");
//	        System.out.println(value.getLength());
//	        out.write(value.getBytes());//用这个就有问题，每个图像的大小都是一个确定的数值，不知道为什么？
	        out.write(value.getBytes(), 0, value.getLength());
	        out.close();
	        
	        exePath = "/home/wangjz/workspace/OpencvTest/Debug/OpencvTest ";
	        String[] commandArgs={"/bin/sh","-c",exePath+fileName};
			String colorful = RunProcess(commandArgs);
			//System.out.println("color:"+colorful);
			//System.out
			context.write(key,new Text(colorful));
	        //context.write(md5Text, key);
		}
	}

	public static class ImageDupsReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
							throws IOException, InterruptedException {
			Text imageFilePath = null;
			for (Text filePath : values) {
				imageFilePath = filePath;
				break;
			}
			context.write(key,imageFilePath);
		}
	}

	public static void main(String[] args) throws Exception {
		//执行参数：hdfs://localhost:9000/user/wangjz/data_out/ hdfs://localhost:9000/user/wangjz/tempOut
		Configuration conf = new Configuration();
		
		//This is the line that makes the hadoop run locally
		//conf.set("mapred.job.tracker", "local");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "image dups remover");
		job.setJarByClass(SequenceFileToImage.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(SeqToImageMapper.class);
		job.setReducerClass(ImageDupsReducer.class);
		//job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
