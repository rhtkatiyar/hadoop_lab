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

public  class EdgeReduce13 extends Reducer<LongWritable, Text, Text, Text>
{       
      int cid[];
      public void setup(Context context) throws IOException, InterruptedException
      {
        cid=new int[Integer.parseInt(context.getConfiguration().get("v"))];
        for(int i=0;i<Integer.parseInt(context.getConfiguration().get("v"));i++)
        cid[i]=2;
        String lines[]=context.getConfiguration().get("mincut").split("\n");
        for(String line:lines)
        {	
			String [] str=line.split("\t");
			for(int i=1;i<str.length;i++)
			{
            cid[Integer.parseInt(str[i])-1]=1;
			}
        }
      }
    
     public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
     {
            for(Text t:values)
            {
                if(cid[Integer.parseInt(t.toString().split("\t")[0])-1]!=cid[Integer.parseInt(t.toString().split("\t")[1])-1])
                {
                    context.write(new Text(t.toString().split("\t")[0]),new Text(t.toString().split("\t")[1]));
                }
            }
     }
}
