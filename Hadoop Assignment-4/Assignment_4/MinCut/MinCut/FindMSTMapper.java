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

public class FindMSTMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{	
	long a, b,p;
	long l;
	protected void setup(Context context) throws IOException,InterruptedException// variable setup class// Define variables
	{
			Configuration conf = context.getConfiguration();
			l = Long.parseLong(conf.get("l"));
			a = Long.parseLong(conf.get("a"));
			b = Long.parseLong(conf.get("b"));
			p = Long.parseLong(conf.get("p"));

	}
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException
	{
		String []inputTokens = value.toString().split("\t");  
		String weight = inputTokens[2] ;
		int wt = Integer.parseInt(weight);
		IntWritable iwWeight = new IntWritable(wt);
		String vertexIn=inputTokens[0];
		int in=Integer.parseInt(vertexIn);
		Integer redid=(int)(((a*in+b)%p)%l);
		String redId=Integer.toString(redid);
		String vertexInOut=redId+":"+inputTokens[0] + ":" + inputTokens[1];
		context.write(iwWeight,new Text(vertexInOut));

			
	}
}
