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
        
public class main
{
	public static enum Lines
	{
		numofLines
	}
	static enum MSTCounters
	{
		totalWeight
	}
	
	public static void main(String[] args) throws Exception
	{
	 
		if(args.length != 1)
		{
			System.out.println("\nUsage : Javafile <Neeta Value>");
			System.exit(0);
		}
		/////////////////////////////////////////////////////////////////////////////////////////////
		boolean firsttime=true;
		boolean flag=true;
		long totalLine=0;
		long n=Long.parseLong(args[0]);
		Configuration conf = new Configuration();
		int file =1;
		Path in,out;
		while(flag)
		{
				Job job = new Job(conf);
				job.setJarByClass(main.class);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				job.setMapperClass(LinesMapper.class);
				job.setReducerClass(LinesReduce.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/temp" + (file - 1) + "/");
				out = new Path("/rohit/temp" + file);
				file++;
				FileInputFormat.addInputPath(job, in);
				FileOutputFormat.setOutputPath(job,out);  
				job.waitForCompletion(true); 
				Counters jobCounters = job.getCounters();	
				long totallines = jobCounters.findCounter(Lines.numofLines).getValue();
				System.out.println("The total lines " + totallines);
				if(totallines==totalLine)
				totalLine=n;
				else
				totalLine=totallines;
				
				Integer I=(int)(totalLine/n);
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////		    
		
			    if(I>1)// totalline>n
			    {
					BigInteger pi;
					long p,a,b;
					int bitLength = 40;
					Random rnd = new Random();
					Integer l=(int)(totalLine/n);
					l=l-1;//comment
					pi = BigInteger.probablePrime(bitLength, rnd);
					p=pi.longValue();
					a=Math.abs((rnd.nextLong()))%p;
					b=Math.abs((rnd.nextLong()))%p;
					conf.set("p",Long.toString( p));
					conf.set("a",Long.toString( a));
					conf.set("b",Long.toString( b));
					conf.set("l",Integer.toString( l));
					Job job1 = new Job(conf);
					job1.setNumReduceTasks(l);
					job1.setJarByClass(main.class);
					job1.setOutputKeyClass(IntWritable.class);
					job1.setOutputValueClass(Text.class);
					job1.setMapperClass(FindMSTMapper.class);
					job1.setPartitionerClass(MyPartitioner.class);
					job1.setReducerClass(FindMSTReducer.class);
					job1.setInputFormatClass(TextInputFormat.class);
					job1.setOutputFormatClass(TextOutputFormat.class);
					in = new Path("/rohit/temp" +( file-1) + "/");
					out = new Path("/rohit/temp" + file);
					file++;
					FileInputFormat.addInputPath(job1, in);
					FileOutputFormat.setOutputPath(job1,out);  
					job1.waitForCompletion(true);
					Counters jobCounters1 = job1.getCounters();	
					long totalWeight = jobCounters1.findCounter(MSTCounters.totalWeight).getValue();
					System.out.println("The weight mst " + totalWeight);
				 
				}
				else
				{
					BigInteger pi;
					long p,a,b;
					int bitLength = 40;
					Random rnd = new Random();
					Integer l=(int)(totalLine/n);
					if(l==0)
					l=1;
					pi = BigInteger.probablePrime(bitLength, rnd);
					p=pi.longValue();
					a=Math.abs((rnd.nextLong()))%p;
					b=Math.abs((rnd.nextLong()))%p;
					conf.set("p",Long.toString( p));
					conf.set("a",Long.toString( a));
					conf.set("b",Long.toString( b));
					conf.set("l",Integer.toString( l));
					Job job1 = new Job(conf);
					job1.setNumReduceTasks(1);
					job1.setJarByClass(main.class);
					job1.setOutputKeyClass(IntWritable.class);
					job1.setOutputValueClass(Text.class);
					job1.setMapperClass(FindMSTMapper.class);
					job1.setPartitionerClass(MyPartitioner.class);
					job1.setReducerClass(FindMSTReducer.class);
					job1.setInputFormatClass(TextInputFormat.class);
					job1.setOutputFormatClass(TextOutputFormat.class);
					in = new Path("/rohit/temp" + (file - 1) + "/");
					out = new Path("/rohit/output");
					FileInputFormat.addInputPath(job1, in);
					FileOutputFormat.setOutputPath(job1,out);  
					job1.waitForCompletion(true);
					Counters jobCounters1 = job1.getCounters();	
					long totalWeight = jobCounters1.findCounter(MSTCounters.totalWeight).getValue();
					System.out.println("The weight mst " + totalWeight);
					flag =false;	
				}
			
		}
		/////////////////////////////////////////////////////////////////////////////////////////////////
	}
}//Class main.java 
