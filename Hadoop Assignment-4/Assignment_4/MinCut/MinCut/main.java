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
		public static enum Lines
	{
		numofLines
	}
	static enum MSTCounters
	{
		totalWeight
	}
	
	static class MinCutthread implements Runnable
	{
		private Thread thrd;
		private  String threadName;
		private String t;
		private String n;
		private String alpa;
		private String no_of_vertex;

		MinCutthread( String name,String n,String t,String alpa,String no_of_vertex)
		{
			this.threadName = name;
			this.t=t;
			this.n=n;
			this.alpa=alpa;
			this.no_of_vertex=no_of_vertex;
		}
	

	
	public  int mainMST(String file,String n) throws Exception
	{
		boolean firsttime=true;
		boolean flag=true;
		long totalLine=0;
		long n1=Long.parseLong(n);
		Configuration conf = new Configuration();
		int file1 =Integer.parseInt(file);
		Path in,out;
		while(flag)
		{
			try
			{
			
			Job job = new Job(conf);
			job.setJarByClass(main.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(LinesMapper.class);
			job.setReducerClass(LinesReduce.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			in = new Path("/rohit/" +threadName+ (file1 - 1) + "/");
			out = new Path("/rohit/"+threadName + file1);
			file1++;
			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job,out);  
			job.waitForCompletion(true); 
			Counters jobCounters = job.getCounters();	
			long totallines = jobCounters.findCounter(main.Lines.numofLines).getValue();
			System.out.println("The total lines inside MST " + totallines);
			if(totallines==totalLine)
				totalLine=n1;
			else
				totalLine=totallines;
				

			Integer I=(int)(totalLine/n1);
			if(I>1)
			{
				BigInteger pi;
				long p,a,b;
				int bitLength = 40;
				Random rnd = new Random();
				Integer l=(int)(totalLine/n1);
				l=l-1;
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
				in = new Path("/rohit/"+threadName +( file1-1) + "/");
				out = new Path("/rohit/"+threadName + file1);
				file1++;
				FileInputFormat.addInputPath(job1, in);
				FileOutputFormat.setOutputPath(job1,out);  
				job1.waitForCompletion(true);
				Counters jobCounters1 = job1.getCounters();	
				long totalWeight = jobCounters1.findCounter(main.MSTCounters.totalWeight).getValue();
				System.out.println("The weight mst " + totalWeight);
				
				 
			}
			
			else
			{
				BigInteger pi;
				long p,a,b;
				int bitLength = 40;
				Random rnd = new Random();
				Integer l=(int)(totalLine/n1);
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
				in = new Path("/rohit/" +threadName+ (file1 - 1) + "/");
				out = new Path("/rohit/"+threadName + file1);
				file1++;
				FileInputFormat.addInputPath(job1, in);
				FileOutputFormat.setOutputPath(job1,out);  
				job1.waitForCompletion(true);
				Counters jobCounters1 = job1.getCounters();	
				long totalWeight = jobCounters1.findCounter(main.MSTCounters.totalWeight).getValue();
				System.out.println("The weight mst " + totalWeight);
				flag =false;	
				
			}
		}catch(Exception e){e.printStackTrace();}
		}//while
		return file1;
	}


	public void run()
	{
//		boolean firsttime=true;
		boolean flag=true;
		long totalLine=0;
		long n1=Long.parseLong(n);
		//double t=0;
		Configuration conf = new Configuration();
		int file =1;
		Path in,out;
		while(flag)
		{
			 try
			 {
				Job job = new Job(conf);
				job.setJarByClass(main.class);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(Text.class);
				job.setMapperClass(LinesMapper.class);
				job.setReducerClass(LinesReduce.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/"+threadName+(file - 1) + "/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job, in);
				FileOutputFormat.setOutputPath(job,out);  
				job.waitForCompletion(true); 
				Counters jobCounters = job.getCounters();	
				long totallines = jobCounters.findCounter(main.Lines.numofLines).getValue();
				System.out.println("The total lines/job before if condition " + totallines);
				totalLine=totallines;
				System.out.println("eta value /n before if " + n1);
				
				if(totalLine<=n1)
				{
					
					Job job5 = new Job(conf);
					job5.setJarByClass(main.class);
					job5.setOutputKeyClass(LongWritable.class);
					job5.setOutputValueClass(Text.class);
					job5.setMapperClass(MapperMinCut.class);
					job5.setReducerClass(ReducerMinCut.class);
					job5.setInputFormatClass(TextInputFormat.class);
					job5.setOutputFormatClass(TextOutputFormat.class);
					in = new Path("/rohit/"+threadName + (file - 1) + "/");
					out = new Path("/rohit/"+threadName+file);
					file++;
					FileInputFormat.addInputPath(job5, in);
					FileOutputFormat.setOutputPath(job5,out);  
					job5.waitForCompletion(true); 
					System.out.println("file_No after mincut/job5  "+file);
					//Use std library to find MinCut and beak from while loop
					
					//Mapping start 13
					conf.set("r",n);
					conf.set("v",no_of_vertex);
					BufferedReader br=new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("/rohit/"+threadName + (file - 1) + "/part-r-00000"))));
					String line="",componentIds="";
					while((line=br.readLine())!=null) componentIds = componentIds+line+"\n";
					conf.set("mincut",componentIds);
					Job job6 =new Job(conf);
					job6.setJarByClass(main.class);
					job6.setMapperClass(EdgeMap13.class);
					job6.setReducerClass(EdgeReduce13.class);
					job6.setPartitionerClass(MyPartitioneredge13.class);
					job6.setNumReduceTasks(Integer.parseInt(n));
					job6.setMapOutputKeyClass(LongWritable.class);
					job6.setMapOutputValueClass(Text.class);
					job6.setOutputKeyClass(Text.class);
					job6.setOutputValueClass(Text.class);
					job6.setInputFormatClass(TextInputFormat.class);
					job6.setOutputFormatClass(TextOutputFormat.class);
					in = new Path("/rohit/temp0/");
					out = new Path("/rohit/finish"+threadName);
					file++;
					FileInputFormat.addInputPath(job6,in);
					FileOutputFormat.setOutputPath(job6,out);
					job6.waitForCompletion(true);
					System.out.println("file_No after finish/job6 "+file);
					//End Mapping CC(u) CC(v)
				    flag=false;
					///
					
					
				}
				else
				{
				conf.set("t",t);//args[1] hold value of t
				Job job1 = new Job(conf);
				job1.setJarByClass(main.class);
				job1.setOutputKeyClass(LongWritable.class);
				job1.setOutputValueClass(Text.class);
				job1.setMapperClass(EMapperMC.class);
				job1.setReducerClass(EReduceMC.class);
				job1.setInputFormatClass(TextInputFormat.class);
				job1.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/"+threadName + (file - 1) + "/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job1, in);
				FileOutputFormat.setOutputPath(job1,out);
 				job1.waitForCompletion(true); 
				file=mainMST(Integer.toString(file),n); 
				System.out.println("file_No V' E' /job1"+file);
				//flag =false;
				//Add missing vertex in V' E' ---> V,E'

				conf.set("nv",no_of_vertex);// Total No of vertex
				Job job2 = new Job(conf);
				//System.out.println("\n"+args[0]);
				job2.setJarByClass(main.class);
				job2.setOutputKeyClass(LongWritable.class);
				job2.setOutputValueClass(Text.class);
				job2.setMapperClass(MapperCV.class);
				job2.setReducerClass(ReduceCV.class);
				job2.setInputFormatClass(TextInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/"+threadName + (file - 1) + "/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job2, in);
				FileOutputFormat.setOutputPath(job2,out);  
				job2.waitForCompletion(true); 
				System.out.println("file_No V E' after add missing vertex/job2  "+file);
				//End  Add missing vertex in V' E' ---> V,E'
				
				//start find connected component V,E'
				
				Job job3 = new Job(conf);
				job3.setJarByClass(main.class);
				job3.setOutputKeyClass(LongWritable.class);
				job3.setOutputValueClass(Text.class);
				job3.setMapperClass(MapperCC.class);
				job3.setReducerClass(ReduceCC.class);
				job3.setInputFormatClass(TextInputFormat.class);
				job3.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/"+threadName + (file - 1) + "/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job3, in);
				FileOutputFormat.setOutputPath(job3,out);  
				job3.waitForCompletion(true); 
				System.out.println("file_No V E' after finding connected components no file is vertexId componentsId /job3  "+file);
				//End find connected component
				
				//Mapping start CC(u) CC(v)
				conf.set("r",n);
				conf.set("v",no_of_vertex);
       			BufferedReader br=new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("/rohit/"+threadName + (file - 1) + "/part-r-00000"))));
				String line="",componentIds="";
				while((line=br.readLine())!=null) componentIds = componentIds+line+"\n";
				conf.set("componentIds",componentIds);
				Job job4 =new Job(conf);
				job4.setJarByClass(main.class);
				job4.setMapperClass(EdgeMap.class);
				job4.setReducerClass(EdgeReduce.class);
				job4.setPartitionerClass(MyPartitioneredge.class);
				job4.setNumReduceTasks(Integer.parseInt(n));
				job4.setMapOutputKeyClass(LongWritable.class);
				job4.setMapOutputValueClass(Text.class);
				job4.setOutputKeyClass(Text.class);
				job4.setOutputValueClass(Text.class);
				job4.setInputFormatClass(TextInputFormat.class);
				job4.setOutputFormatClass(TextOutputFormat.class);
				in = new Path("/rohit/temp0/");
				out = new Path("/rohit/"+threadName+file);
				file++;
				FileInputFormat.addInputPath(job4,in);
				FileOutputFormat.setOutputPath(job4,out);
				job4.waitForCompletion(true);
				System.out.println("file_No after Mapping /job4 "+file);
				}
			}catch(Exception e){e.printStackTrace();}
		}//while
	}//run
	
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
			System.out.println(" \n Usage : Javafile <Eeta Value> <t value> <alpa value> <total no of vertex> ");
			System.exit(0);
		}
		for(int i=0;i<Integer.parseInt(args[2]);i++)
		{
			MinCutthread R = new MinCutthread( "Thread"+i,args[0],args[1],args[2],args[3]);
			R.start();
		}
	}
}//Class main.java 
