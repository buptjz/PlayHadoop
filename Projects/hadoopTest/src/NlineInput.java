import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NlineInput extends Configured implements Tool {
    
    public static class MapClass extends MapReduceBase
        implements Mapper<Object, NLineInputFormat, Text, HMapSIW> {

    	private static final HMapSIW MAP = new HMapSIW();
    	private String[] itemsInLine;
    	private String user;
    	private String spName;
        public void map(Object key, NLineInputFormat value,
                        OutputCollector<Text, HMapSIW> output,
                        Reporter reporter) throws IOException {
        	
        	String[] lines;
        	String line;
        	lines = value.toString().split("\\n");
        	for(int i =0;i<lines.length;i++){
        		MAP.clear();
        		line = lines[i];
            	itemsInLine = line.split("\\t");
                //	0					1			2				3				4		5		6
                //2012-07-05 00:06:20	319b7db6	60.28.212.62	hdn.xnimg.cn:80	xnimg	1065	18816
            	user = itemsInLine[1];
            	spName = itemsInLine[4];
            	MAP.increment(spName);
                output.collect(new Text(user), MAP);
        	}
        }
    }
    
    
    public static class Reduce extends MapReduceBase
        implements Reducer<Text, HMapSIW, Text, HMapSIW> {


        public void reduce(Text key, Iterator<HMapSIW> values,
                           OutputCollector<Text, HMapSIW> output,
                           Reporter reporter) throws IOException {
            Iterator<HMapSIW> iter = values;

            HMapSIW map = new HMapSIW();

            while (iter.hasNext()) {
              map.plus(iter.next());
            }
            output.collect(key, map);
        }
    }
    
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        JobConf job = new JobConf(conf, NlineInput.class);
        
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("NlineInput");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormat(NLineInputFormat.class);

        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HMapSIW.class);
        JobClient.runJob(job);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception { 
        int res = ToolRunner.run(new Configuration(), new NlineInput(), args);
        
        System.exit(res);
    }
}