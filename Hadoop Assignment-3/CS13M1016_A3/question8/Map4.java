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

        
public class Map4 extends Mapper<LongWritable, Text , Text, Text>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
		String line = value.toString();
        String outlink="";
        //StringTokenizer tokenizer = new StringTokenizer(line);
        String [] tokenizer =line.split("	");
   
		for(int k=0;k<tokenizer.length;k++)
		{
           if(outlink=="")
              outlink=outlink+tokenizer[k];
            else
			  outlink=outlink+","+tokenizer[k];
         }   
        
        outlink="PrimaryId"+","+outlink;
        context.write(new Text("PrimaryId"), new Text(outlink));
     }
        
		
}
//Class MAP
 
 
 

