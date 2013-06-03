/**尝试将ToSeqFile类打包成的SeqFile读取出来，但是好像读取的有问题
 * 所以我信件了一个工程HadoopWithSequenceFile来继续研究，这个文件就暂时放在一边了
 * **/
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadDemo {
    public static void main(String[] args) throws IOException {
    	
    	Configuration conf = new Configuration();
        Path path = new Path("/home/wangjz/Desktop/part-r-00001");
        SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
        
        
        //String uri = args[0];
//        String uri = "hdfs://localhost:9000/user/wangjz/outdata/";
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(URI.create(uri), conf);
//        Path path = new Path(uri);
//
//        
//        SequenceFile.Reader reader = null;
        try {
            //reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);
            
            long position = reader.getPosition();
            while (reader.next(key, value)) {//循环读取文件\
            	
                String syncSeen = reader.syncSeen() ? "*" : "";//SequenceFile中都有sync标记
//                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,
//                        value);
                System.out.printf("[%s\t%s\t%s］\n", position, key,syncSeen);
                position = reader.getPosition(); //下一条record开始的位置
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}