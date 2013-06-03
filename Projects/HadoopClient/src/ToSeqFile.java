/**
 *这是在网上找到了将许多小文件转换成 sequence File的方式
 *最开始job.setNumReduceTasks(0);的时候只能得到若干个part
 *而将其改成1的时候
 * **/
//http://stackoverflow.com/questions/5377118/how-to-convert-txt-file-to-hadoops-sequence-file-format

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ToSeqFile extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		System.out.println(System.getProperty("java.library.path"));
		Job job = new Job();
		job.setJarByClass(getClass());
		Configuration conf=getConf();
		FileSystem fs = FileSystem.get(conf);

		FileInputFormat.setInputPaths(job, "hdfs://localhost:9000/user/wangjz/data_in/");
		Path outDir=new Path("/home/wangjz/Desktop/1");
		fs.delete(outDir,true);
		//FileOutputFormat.setOutputPath(job, outDir);
		
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/wangjz/outdata"));

		// increase if you need sorting or a special number of files
		//job.setNumReduceTasks(0);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		//设置OutputFormat为SequenceFileOutputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//允许压缩
		SequenceFileOutputFormat.setCompressOutput(job, true);
		//压缩算法为gzip
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		//压缩模式为BLOCK
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);


		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ToSeqFile(), args);
		System.exit(res);
	}
}