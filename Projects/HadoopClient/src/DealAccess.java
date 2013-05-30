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
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DealAccess extends Configured implements Tool {
    
    public static class MapClass extends MapReduceBase
        implements Mapper<Text, Text, Text, IntWritable> {

    	private IntWritable traffic = new IntWritable();
    	private int totaltraffic = 0;
    	private String[] records;
    	private String userSP;
        public void map(Text key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            records = value.toString().split("\\s");
            //						0			1				2				3		4		5
            //2012-07-05 00:06:20	319b7db6	60.28.212.62	hdn.xnimg.cn:80	xnimg	1065	18816
            userSP = records[0]+"\t"+records[3];
            totaltraffic = Integer.parseInt(records[5])+Integer.parseInt(records[4]);
        	traffic.set(totaltraffic);
            output.collect(new Text(userSP), traffic);
        }
    }
    
    public static class Reduce extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, Text> {

    	private String finalValue ;
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
                           
            int traffic = 0;
            int times = 0;
            while (values.hasNext()) {
                traffic += values.next().get();
                times = times + 1;
            }
            finalValue = '\t'+String.valueOf(traffic)+'\t'+String.valueOf(times);
            output.collect(key, new Text(finalValue));
        }
    }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        JobConf job = new JobConf(conf, DealAccess.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("DealAccess");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.set("key.value.separator.in.input.line", ",");
        JobClient.runJob(job);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new DealAccess(), args);
        
        System.exit(res);
    }
}