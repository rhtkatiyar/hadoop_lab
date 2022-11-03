import java.io.IOException;
import java.math.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


        
public class main {
	public static void main(String[] args) throws Exception {
	 
	if(args.length != 3)
    {
		System.out.println("\nUsage : Javafile  <inputdir> <outputdir>");
		System.exit(0);
	}
    
    Configuration conf = new Configuration();
    //conf.set("s1", args[2]);
    //conf.set("c1",args[3]); 
    Job job = new Job(conf,"main");
    job.setNumReduceTasks(1);
    job.setJarByClass(main.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map4.class);
    job.setMapperClass(Map8.class);
    job.setReducerClass(Reduce4.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,Map4.class);
    MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,Map8.class);
    
    FileOutputFormat.setOutputPath(job,new Path(args[2]));
    job.waitForCompletion(true); 	
	}
}//Class main.java 

