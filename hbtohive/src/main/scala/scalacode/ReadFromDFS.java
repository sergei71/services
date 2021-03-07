package scalacode;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

public class ReadFromDFS {
    public static String readHdfs(String p) throws IOException,URISyntaxException{
        Path path = new Path(p);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://nameservice1"),conf);
        String res = "";
        InputStream in = null;
        try{
            in = fs.open(path);
            BufferedReader br=new BufferedReader(new InputStreamReader(in));
            String str = "";
            while((str=br.readLine())!=null){
		        res+=str;
            }
        }
        catch (Exception e){

        }
        return res;
    }

    public static void writefromread() throws IOException,URISyntaxException{
        String data = readHdfs("hdfs://<path-in-hdfs>/conf.json");
    }

    public static void main(String[] args) throws IOException,URISyntaxException{
        String data = readHdfs("hdfs://<path-in-hdfs>/conf.json");
    }
}
