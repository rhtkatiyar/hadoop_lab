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
	public static void main(String[] args) throws Exception {
	 
	if(args.length != 2)
    {
		System.out.println("\nUsage : Javafile  <inputdir> <outputdir> <s1> <c1>");
		System.exit(0);
	}
    
    Configuration conf = new Configuration();
   // conf.set("s1", args[2]);
   // conf.set("c1",args[3]); 
    Job job = new Job(conf,"main");
    job.setJarByClass(main.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    job.waitForCompletion(true); 	
	}
}//Class main.java 

