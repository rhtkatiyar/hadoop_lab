import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

       

public class Reduce  extends Reducer<LongWritable, Text, LongWritable,Text>
{
     public void reduce(LongWritable key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
		{ 
				String vertexIn="";
				for (Text val : values)
				{
					if(vertexIn=="")
					vertexIn =vertexIn + val.toString();
					else
					vertexIn=vertexIn+"	"+val.toString();
					
				}
				     
				context.write(key, new Text(vertexIn));
                     
		}
				

}	

