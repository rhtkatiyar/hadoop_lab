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
import org.jgrapht.graph.*;
import org.jgrapht.alg.*;
       

public class ReduceCV  extends Reducer<LongWritable,Text, LongWritable,Text>
{
	int v;
	long nv;
	protected void setup(Context context) throws IOException,InterruptedException// variable setup class// Define variables
	{
			Configuration conf = context.getConfiguration();
			nv = Long.parseLong(conf.get("nv"));
			v = (int)nv;
			

	}

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
	{ 
		for(Text val:values)
		{	
			context.write(key, new Text(val));

		}
	}
	protected void cleanup(Context context) throws IOException,InterruptedException
				{
					for(int i=1;i<=v;i++)
					{
						context.write(new LongWritable(i),new Text(""+i+"\t"+"1"));
					}
					
				}

}	

