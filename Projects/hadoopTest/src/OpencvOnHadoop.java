/**
 * 这个测试主要是考虑到在Hadoop中使用opencv提取图像特征
 * 参考网上的例子，实现了Hadoop与本地其他应用程序的衔接
 * 《Hadoop 运行 c++ 程序实验》
 * http://www.cnblogs.com/Donal/archive/2012/03/09/2387873.html
 * 作者调用了C＋＋程序，我在这里调用了OPENCV程序，名字叫做OPENCVTest
 * 我尝试了调用python程序，也是成功的
 * **/

import java.io.IOException;
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
    public static int RunProcess(String[] args){
        int exitcode = -1;
        System.out.println(Arrays.toString(args));  
        try{
            Runtime runtime=Runtime.getRuntime(); 
            final Process process=runtime.exec(args);
            // any error message?
            new StreamGobbler(process.getErrorStream(), "ERROR").start();
            // any output?
            new StreamGobbler(process.getInputStream(), "OUTPUT").start();
            process.getOutputStream().close();
            exitcode=process.waitFor();
        }catch (Throwable t){
            t.printStackTrace();
        }
        return exitcode;
    }
    
    
    public static class MapClass extends MapReduceBase
        implements Mapper<Object, Text, Text, IntWritable> {

    	private final static IntWritable one = new IntWritable(1);
    	private String name,hdfsPath,localInPath,hadoopPath,localOutPath,hdfstmpPath,exePath;
        public void map(Object key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
        	//输入的是一个文档，文档中的每一行是一个文件的名字，作为mapper的value值。
        	name = value.toString();
        	hdfsPath = "data_in/images/"+name;
        	localInPath = "~/Desktop/localImage/"+name;
        	localOutPath ="~/Desktop/localOut/"+name;
        	hadoopPath = "/home/wangjz/hadoop-1.0.4/bin/hadoop";
        	hdfstmpPath = "data_in/temp/"+name;
        	exePath = "/home/wangjz/Desktop/OpencvTest ";
        	//Step 1 : 将HDFS上的文件拷贝到本地
            RunProcess(new String[]{"/bin/sh","-c",hadoopPath+" fs -copyToLocal " +hdfsPath+" "+localInPath});
            //Step 2 : 设置命令，deal.py是一个处理字符串的程序，程序本身有输入和输出
            String[] commandArgs={"/bin/sh","-c",exePath+localInPath+" "+localOutPath};
            RunProcess(commandArgs);
            //Step 3 : 将处理好的本地文件回传给hdfs
            RunProcess(new String[]{"/bin/sh","-c",hadoopPath+" fs -copyFromLocal "+localOutPath+" "+hdfstmpPath});
            
            output.collect(new Text(name), one);
        }
    }
    
    public static class Reduce extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
        	output.collect(key,new Text(""));
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
        job.setOutputValueClass(IntWritable.class);
        JobClient.runJob(job);
        return 0;
    }
    
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new OpencvOnHadoop(), args);
        System.exit(res);
    }
}