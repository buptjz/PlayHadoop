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

public class CPPTest extends Configured implements Tool {
    
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
    	private String name,remotePath,localPath,hadoopPath,localOutPath;
        public void map(Object key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
        	//输入的是一个文档，文档中的每一行是一个文件的名字，作为mapper的value值。
        	name = value.toString();
        	remotePath = "data_in/"+name;
        	localPath = " ~/Desktop/local/"+name;
        	localOutPath =" ~/Desktop/out/"+name;
        	hadoopPath = "/home/wangjz/hadoop-1.0.4/bin/hadoop";
        	
        	//将HDFS上的文件拷贝到本地
            RunProcess(new String[]{"/bin/sh","-c",hadoopPath+" fs -copyToLocal " +remotePath+localPath});
            
            //设置命令，deal.py是一个处理字符串的程序，程序本身有输入和输出
            String[] commandArgs={"/bin/sh","-c","python /home/wangjz/Desktop/deal.py"+localPath+localOutPath};
            
            //执行本地命令
            RunProcess(commandArgs);
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
        JobConf job = new JobConf(conf, CPPTest.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("CPPTest");
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
        int res = ToolRunner.run(new Configuration(), new CPPTest(), args);
        System.exit(res);
    }
}