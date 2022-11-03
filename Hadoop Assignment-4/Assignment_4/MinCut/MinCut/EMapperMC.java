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

public class EMapperMC extends Mapper<LongWritable, Text, LongWritable,Text>
{
	
	static double t=0;
	
	protected void setup(Context context) throws IOException,InterruptedException// variable setup class// Define variables
	{
			Configuration conf = context.getConfiguration();
			t = Double.parseDouble(conf.get("t"));

	}
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
		{
			String []inputTokens = value.toString().split("\t");  
			String weight = "1";//inputTokens[2] ;
			String vertexIn=inputTokens[0];
			String vertexOut=inputTokens[1];
		    double min=0;
		    double max=1;
		    Random generator = new Random();
			double num = generator.nextDouble()*(max-min) + min;
			if(num<t)
			context.write(new LongWritable(Long.parseLong(vertexIn.toString())),new Text(vertexOut+"\t"+weight+"\t"+Double.toString(num)));
		}
}
