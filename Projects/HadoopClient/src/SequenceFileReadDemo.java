/**尝试将ToSeqFile类打包成的SeqFile读取出来，但是好像读取的有问题
 * 所以我信件了一个工程HadoopWithSequenceFile来继续研究，这个文件就暂时放在一边了
 * 恩，我用BinaryFilesToHadoopSequenceFile来生成sequenceFile的时候就可以没问题
 * 输出的结果也是正确的!
 * **/
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadDemo {
    public static void main(String[] args) throws IOException {
    	
    	Configuration conf = new Configuration();
        Path path = new Path("/home/wangjz/Desktop/part-r-00001");
        SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
        try {
            Writable key = (Writable) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            //因为图像数据是二进制的方式存储到sequenceFile中的，所以要用二进制的方式读取进来
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);
            
            long position = reader.getPosition();
            while (reader.next(key, value)) {//循环读取文件,将文件保存到本地
                String syncSeen = reader.syncSeen() ? "*" : "";//SequenceFile中都有sync标记
                System.out.printf("[%s\t%s\t%s］\n", position, key,syncSeen);
                String fileName = "/home/wangjz/Desktop/output/"+key.toString().split("/")[6];
                DataOutputStream out=new DataOutputStream(new FileOutputStream(fileName));  
                out.write(value.getBytes());
                out.close();  
                position = reader.getPosition(); //下一条record开始的位置
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}