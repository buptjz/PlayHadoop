/**
 * OpencvOnHadoop的辅助类
 * 通过流的方式使得Java程序与外部程序进行通信
 * **/

import java.io.*;
class StreamGobbler extends Thread
{
    InputStream is;
    String type;
    StreamGobbler(InputStream is, String type)
    {
        this.is = is;
        this.type = type;
    }

    public void run()
    {
        try
        {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line=null;
            while ((line = br.readLine()) != null)
                System.out.println(type + ">" + line);
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }
}
