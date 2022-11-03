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

        
public class Map8 extends Mapper<LongWritable, Text , Text, Text>
{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
		String line = value.toString();
        String outlink="";
        //StringTokenizer tokenizer = new StringTokenizer(line);
        String [] line1=line.split("	");
        String [] tokenizer =line1[0].split(",");
   
		for(int k=0;k<tokenizer.length;k++)
		{
           if(outlink=="")
              outlink=outlink+tokenizer[k];
            else
			  outlink=outlink+","+tokenizer[k];
         }   
			outlink="UniqueId"+","+outlink;
            context.write(new Text("UniqueId"), new Text(outlink));
     }
        
		
}
//Class MAP
 
 
 

