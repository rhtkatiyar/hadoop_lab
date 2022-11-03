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


        
public class Map extends Mapper<LongWritable, Text , LongWritable, Text>
{

	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
		Text vertexIn=new Text();
        String line = value.toString();
        String outlink="";
        StringTokenizer tokenizer = new StringTokenizer(line);
        vertexIn.set(tokenizer.nextToken());
        while (tokenizer.hasMoreTokens())
       {
           if(outlink=="")
              outlink=outlink+tokenizer.nextToken();
            else
			  outlink=outlink+"	"+tokenizer.nextToken();
            
        }
        context.write(new LongWritable(Long.parseLong(vertexIn.toString())), new Text(outlink));
        

    }
        
		
}
//Class MAP
 
 
 

