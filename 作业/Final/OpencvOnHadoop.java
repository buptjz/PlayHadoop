/**
 *作为实验的对比项，不使用sequenceFile,单独处理每一个图像文件
 * **/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

public class OpencvOnHadoop extends Configured implements Tool {
    
	//运行本地函数的程序
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
    
    public static class MapClass extends MapReduceBase
        implements Mapper<Object, Text, Text, Text> {

    	private String name,hdfsPath,localInPath,hadoopPath,exePath;
        public void map(Object key, Text value,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {
        	//输入的是一个文档，文档中的每一行是一个文件的名字，作为mapper的value值。
        	name = value.toString();
        	hdfsPath = "data_in/1000/"+name;
        	localInPath = "~/Desktop/local1000Image/"+name;
        	hadoopPath = "/home/wangjz/hadoop-1.0.4/bin/hadoop";
        	exePath = "/home/wangjz/workspace/OpencvTest/Debug/OpencvTest ";
        	//Step 1 : 将HDFS上的文件拷贝到本地
            RunProcess(new String[]{"/bin/sh","-c",hadoopPath+" fs -copyToLocal " +hdfsPath+" "+localInPath});
            //System.out.println(localInPath);
            //Step 2 : 设置命令，deal.py是一个处理字符串的程序，程序本身有输入和输出
            String[] commandArgs={"/bin/sh","-c",exePath+localInPath};
            String colorful = RunProcess(commandArgs);
            
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
        JobConf job = new JobConf(conf, OpencvOnHadoop.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("OpencvOnHadoop");
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
        int res = ToolRunner.run(new Configuration(), new OpencvOnHadoop(), args);
        System.exit(res);
    }
}
