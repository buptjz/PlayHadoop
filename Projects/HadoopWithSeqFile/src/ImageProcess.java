/**
 * 这个测试主要是考虑到在Hadoop中使用opencv提取图像特征
 * 参考网上的例子，实现了Hadoop与本地其他应用程序的衔接
 * 《Hadoop 运行 c++ 程序实验》
 * http://www.cnblogs.com/Donal/archive/2012/03/09/2387873.html
 * 作者调用了C＋＋程序，我在这里调用了OPENCV程序，名字叫做OPENCVTest
 * 我尝试了调用python程序，也是成功的
 * **/

import java.io.*;
import java.util.Iterator;
import java.lang.Runtime;
import java.util.Arrays;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImageProcess extends Configured implements Tool {

	//运行本地函数的程序
	public static String RunProcess(String[] args){
		int exitcode = -1;
		String outputValue = new String();
		//System.out.println(Arrays.toString(args));  
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
		//        try{
			//            Runtime runtime=Runtime.getRuntime(); 
		//            final Process process=runtime.exec(args);
		//            // any error message?
		//            new StreamGobbler(process.getErrorStream(), "ERROR").start();
		//            // any output?
		//            new StreamGobbler(process.getInputStream(), "OUTPUT").start();
		//            process.getOutputStream().close();
		//            exitcode=process.waitFor();
		//        }catch (Throwable t){
		//            t.printStackTrace();
		//        }
		return outputValue;
	}

	public static class MapClass extends MapReduceBase
	implements Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private String name,hdfsPath,localInPath,hadoopPath,exePath;
		public void map(Object key, Text value,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			//输入的是一个文档，文档中的每一行是一个文件的名字，作为mapper的value值。
			name = value.toString();
			hdfsPath = "data_in/"+name;
			localInPath = "~/Desktop/localImage/"+name;

			hadoopPath = "/home/wangjz/hadoop-1.0.4/bin/hadoop";
			exePath = "/home/wangjz/workspace/OpencvTest/Debug/OpencvTest ";
			//Step 1 : 将HDFS上的文件拷贝到本地
			RunProcess(new String[]{"/bin/sh","-c",hadoopPath+" fs -copyToLocal " +hdfsPath+" "+localInPath});
			//Step 2 : 设置命令，deal.py是一个处理字符串的程序，程序本身有输入和输出
			String[] commandArgs={"/bin/sh","-c",exePath+localInPath};
			String colorful = RunProcess(commandArgs);
			//System.out
			output.collect(value,new Text(colorful));
		}
	}

	public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			output.collect(key,values.next());
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, ImageProcess.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setJobName("ImageProcess");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		JobClient.runJob(job);
		return 0;
	}

	public static void main(String[] args) throws Exception { 
		int res = ToolRunner.run(new Configuration(), new ImageProcess(), args);
		System.exit(res);
	}
}