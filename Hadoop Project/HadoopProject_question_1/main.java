import java.io.*;
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
	static class Gisthread implements Runnable
	{
		private Thread thrd;
		private String threadName;
		private String alpa;
		private String numclass;
		private String k;
		private String records;

		Gisthread( String name,String numclass,String alpa,String k,String records)
		{
			this.threadName = name;
			this.alpa=alpa;
			this.numclass=numclass;
			this.k=k;
			this.records=records;
		}
		public void run()
		{
		boolean flag=true;
	//	long totalLine=0;
//long n1=Long.parseLong(n);
		Configuration conf = new Configuration();
		int file =1;
		Path in,out;
			 try
			 {
				BufferedReader br=new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("/rohit/temp1/userInput"))));
				String line="",userInput="";
				while((line=br.readLine())!=null) userInput = userInput+line+"\n";
				
				conf.set("userInput",userInput);
				conf.set("numclass",numclass);
				conf.set("k",k);
				Job job =new Job(conf);
				job.setJarByClass(main.class);
				job.setNumReduceTasks(Integer.parseInt(numclass));
				job.setMapperClass(GisMap.class);
				job.setReducerClass(GisReduce.class);
				job.setPartitionerClass(MyPartitioner.class);
				//job.setNumReduceTasks(Integer.parseInt(numclass));
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/" +threadName+ (file - 1) + "/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job,in);
				FileOutputFormat.setOutputPath(job,out);
				job.waitForCompletion(true);
				System.out.println("Finish numclass part  "+file);
				
				//*************************************************
				Job job1 =new Job(conf);
				job1.setJarByClass(main.class);
				job1.setMapperClass(GisMapCombine.class);
				job1.setReducerClass(GisReduceCombine.class);
				//job.setPartitionerClass(MyPartitioner.class);
				job1.setNumReduceTasks(1);
				job1.setMapOutputKeyClass(LongWritable.class);
				job1.setMapOutputValueClass(Text.class);
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(Text.class);
				job1.setInputFormatClass(TextInputFormat.class);
				job1.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/" +threadName+ (file - 1) + "/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job1,in);
				FileOutputFormat.setOutputPath(job1,out);
				job1.waitForCompletion(true);
				System.out.println("Finish combine 250k  "+file);
				
				//*************************************************
				//*************************************************
				BufferedReader br1=new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("/rohit/" +threadName+ (file - 1) + "/part-r-00000"))));
				String line1="",combineoutput="";
				while((line1=br1.readLine())!=null) combineoutput = combineoutput+line+"\n";
				conf.set("combineoutput",combineoutput);
				conf.set("records",records);
				Job job2 =new Job(conf);
				job2.setJarByClass(main.class);
				job2.setMapperClass(GisMapMerge.class);
				job2.setReducerClass(GisReduceMerge.class);
				job2.setNumReduceTasks(1);
				job2.setMapOutputKeyClass(LongWritable.class);
				job2.setMapOutputValueClass(Text.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				job2.setInputFormatClass(TextInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/temp0/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job2,in);
				FileOutputFormat.setOutputPath(job2,out);
				job2.waitForCompletion(true);
				System.out.println("Finish Merge "+file);
				//*************************************************
			}catch(Exception e){e.printStackTrace();}
		}//while
	//}//run
//}
	public void start ()
	{
		System.out.println("Starting " +  threadName );
		if (thrd == null)
		{
			thrd = new Thread (this, threadName);
			thrd.start ();
		}
	}

	
	
}//class MinCutthread

	public static void main(String[] args) throws Exception
	{
		if(args.length != 4)
		{
			System.out.println(" \n Usage : Javafile <Class Value> <alpa value> <k value> <numofusers>");
			System.exit(0);
		}
		for(int i=0;i<Integer.parseInt(args[1]);i++)
		{
			Gisthread R = new Gisthread( "Thread"+i,args[0],args[1],args[2],args[3]);// args[0]= class, args[1]=alpa, args[2]=k,args[3]=records;
			R.start();
		}
	}
}//Class main.java 
