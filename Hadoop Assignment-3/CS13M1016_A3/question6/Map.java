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

        String line = value.toString();
        String[] tokenizer=line.split("	");
        for(int i=1;i<tokenizer.length;i++)
        {
			String vertexIn="";
			String outlink="";
			for(int k=1;k<tokenizer.length;k++)
			{
				if(outlink=="")
				outlink=outlink+tokenizer[k];
				else
				outlink=outlink+","+tokenizer[k];
			}
			vertexIn=tokenizer[i];
			context.write(new LongWritable(Long.parseLong(vertexIn.toString())), new Text(outlink));
			
		}
       
        
        

    }
        
		
}
//Class MAP
 
 
 

