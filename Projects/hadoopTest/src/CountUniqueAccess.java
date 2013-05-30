import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountUniqueAccess extends Configured implements Tool {
    
    public static class MapClass extends MapReduceBase
        implements Mapper<Object, Text, Text, IntWritable> {

    	private final static IntWritable one = new IntWritable(1);
    	private String[] itemsInLine;
    	private String spName;
        public void map(Object key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
        	itemsInLine = value.toString().split("\\t");
            //	0		1
        	//0b98ba27	sina	
        	spName = itemsInLine[1];
            output.collect(new Text(spName), one);
        }
    }
    
    public static class Reduce extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, Text> {


        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
        	int count = 0;
        	while(values.hasNext()){
        		values.next();
        		count += 1;
        	}
        	output.collect(key,new Text(String.valueOf(count)));
        }
    }
    
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        
        JobConf job = new JobConf(conf, CountUniqueAccess.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("CountUniqueAccess");
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
        int res = ToolRunner.run(new Configuration(), new CountUniqueAccess(), args);
        
        System.exit(res);
    }
}