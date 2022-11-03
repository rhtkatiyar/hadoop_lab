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

       

public class EReduceMC  extends Reducer<LongWritable,Text, LongWritable,Text>
{
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
	{ 
		for(Text val:values)
		{	
			//context.getCounter(main.Lines.numofLines).increment(1);
			context.write(key, new Text(val));
		}
	}
				

}	

