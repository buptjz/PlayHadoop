/**
 * 这个测试主要是考虑到在Hadoop中使用opencv提取图像特征
 * 参考网上的例子，实现了Hadoop与本地其他应用程序的衔接
 * 《Hadoop 运行 c++ 程序实验》
 * http://www.cnblogs.com/Donal/archive/2012/03/09/2387873.html
 * 作者调用了C＋＋程序，我在这里调用了OPENCV程序，名字叫做OPENCVTest
 * 我尝试了调用python程序，也是成功的
 * **/

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class ImageProcessWithSeq {

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

	public static class MapClass extends Mapper<Object, BytesWritable, Text, Text> {
		private String name,localInPath,exePath;
		
		public void map(Text key, BytesWritable value, Context context) throws IOException,InterruptedException {

	        //将文件保存在本地目录中
	        String fileName = "/home/wangjz/Desktop/localImages/"+key.toString().split("/")[6];
	        System.out.println(fileName);
	        DataOutputStream out=new DataOutputStream(new FileOutputStream(fileName));  
	        out.write(value.getBytes());
	        out.close();  
	        
	        exePath = "/home/wangjz/workspace/OpencvTest/Debug/OpencvTest ";
	        localInPath = "~/Desktop/localImages/"+name;
	        String[] commandArgs={"/bin/sh","-c",exePath+localInPath};
			String colorful = RunProcess(commandArgs);
			//System.out
			context.write(key,new Text(colorful));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			output.collect(key,values.next());
		}
	}
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration();
		
		//This is the line that makes the hadoop run locally
		//conf.set("mapred.job.tracker", "local");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "dealwithSeq");
		
		job.setJarByClass(ImageProcessWithSeq.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}