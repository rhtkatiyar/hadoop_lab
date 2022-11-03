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

public class MyPartitioner extends Partitioner<LongWritable , Text>// find the reducer id from mapper output
{
	int num_class;
/*	int []loadbalancer;//=new int[501];
	protected void setup(Context context) throws IOException,InterruptedException // variable setup class // Define variables
	{
			Configuration conf = context.getConfiguration();
			num_class = Integer.parseInt(conf.get("numclass"));
			loadbalancer=new int[num_class];
			for(int i=0;i<num_class;i++)
			{
				loadbalancer[i]=0;
			}
	}*/
	public int getPartition(LongWritable key , Text value , int num_of_reducer)
	{
		String [] valholdrid = value.toString().split("\t");
		int reducernum = Integer.parseInt(valholdrid[0]);
		//loadbalancer[reducernum-1]=loadbalancer[reducernum]+1;
		return (reducernum%(num_of_reducer));
	}
}
