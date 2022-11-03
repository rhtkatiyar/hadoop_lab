import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class EdgeMap extends Mapper<LongWritable,Text, LongWritable,Text>
{
        //input graph file
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
            int r=Integer.parseInt(context.getConfiguration().get("r"));
            String edge[]=value.toString().split("\t");
            int h=(Integer.parseInt(edge[0])%r+Integer.parseInt(edge[1])%r)%r;
            //System.out.println(edge[0]+","+edge[1]+" -> "+h);
            context.write(new LongWritable(h),new Text(value));
        }
        
}
