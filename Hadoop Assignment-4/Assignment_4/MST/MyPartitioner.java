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
public class MyPartitioner extends Partitioner<IntWritable , Text>// find the reducer id from mapper output
{
	public int getPartition(IntWritable key , Text value , int num_of_reducer)
	{
		String [] valholdrid = value.toString().split(":");
		int reducernum = Integer.parseInt(valholdrid[0]);
		return reducernum;	
	}	 
}
