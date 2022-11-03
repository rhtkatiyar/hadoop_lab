import java.io.IOException;
import java.util.*;
import java.math.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

       

public class Map  extends Mapper<LongWritable, Text, Text,Text>
{
	
	   public void map(LongWritable key, Text value, Context context) throws  IOException ,InterruptedException 
		{ 
			
				
				
				String line=value.toString();
				String[] tokenizer=line.split("	");
				for(int i=1;i<tokenizer.length;i++)
				{
					context.write(new Text(tokenizer[0]), new Text(tokenizer[i]));
				}
			

			
		}
		
}	

