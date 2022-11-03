import java.io.IOException;
import java.math.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class main {
	static enum loop{stillleft};
	public static void main(String[] args) throws Exception {
	 boolean recursion=true;
	 long counter=1;
	 int counter1=1;
	 //String input=args[0];
	 //String output=args[1];
	 int depth=7;
	 Path in,out;
	 
	if(args.length != 0)
    {
		System.out.println("\nUsage : Javafile");
		System.exit(0);
	}
    
    Configuration conf = new Configuration();
   // conf.set("s1", args[2]);
   // conf.set("c1",args[3]); 
   while(recursion)
   {
	in = new Path("/input/input" + depth);
	depth++;
    out = new Path("/input/input" + depth);
    //depth++;   
    Job job = new Job(conf,"main");
    job.setJarByClass(main.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job,out);
    job.waitForCompletion(true); 
    if(counter1==1)
    counter = job.getCounters().findCounter(loop.stillleft).getValue();	
    if(counter1==0)
	recursion=false;
    if(counter==0)
    {counter1=0;
	counter=1;	
    recursion=true;
	}
	
	}
}
}//Class main.java 

