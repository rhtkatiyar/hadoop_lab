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
public class Result{
	public enum EDGE
	{
		COUNT
    }
	public static class EDGEMap extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			context.getCounter(Result.EDGE.COUNT).increment(1);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		int minEdges=Integer.MAX_VALUE,threadId=0,threadIdtemp=0;
		int r[]=new int[Integer.parseInt(args[0])];
		Path in,out;
		Configuration conf=new Configuration();
		for(int i=0;i<Integer.parseInt(args[0]);i++)
		{
			r[i]=0;
		Job job = new Job(conf);
		job.setJarByClass(Result.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		in = new Path("/rohit/finishThread"+i);
		out = new Path("/rohit/Mincut"+i);
		//file++;
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job,out);  
		job.waitForCompletion(true); 
		System.out.println("File Merging Done for "+i);
		}
		
		for(int i=0;i<Integer.parseInt(args[0]);i++)
		{
			Job job1 =new Job(conf);
			job1.setJarByClass(Result.class);
			job1.setMapperClass(EDGEMap.class);
			job1.setNumReduceTasks(0);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job1, new Path("/rohit/Mincut"+i));
			FileOutputFormat.setOutputPath(job1, new Path("/rohit/ResultMincut"+i));
			job1.waitForCompletion(true);
			r[i]=(int)job1.getCounters().findCounter(Result.EDGE.COUNT).getValue();
			if(job1.getCounters().findCounter(Result.EDGE.COUNT).getValue()<minEdges)
			{
				minEdges=(int)job1.getCounters().findCounter(Result.EDGE.COUNT).getValue();
				threadId=i;
				//r[i]=minEdges;
				threadIdtemp=minEdges;
			}
		}
		for(int i=0;i<Integer.parseInt(args[0]);i++)
		{
			if(r[i]==threadIdtemp)
			{
				threadId=i;
		BufferedReader br=new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("/rohit/Mincut"+threadId+"/part-r-00000"))));
		String line="";
		String edges="";
		while((line=br.readLine())!=null) edges = edges+line+"\n";
		System.out.println("\n\n****************MinCut Solution*************\n"+i);
		System.out.println(edges);
		System.out.println("******************************************************");	
		}
		
		}
		
	}
	

}
