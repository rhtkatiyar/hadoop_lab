import java.io.IOException;
import java.util.*;
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

	private Text vertexIn;
    private Text vertexOut;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
		String line = value.toString();
		String[] tokenizer = line.split("	");
        vertexIn= new Text(tokenizer[0]);
        vertexOut=new Text(tokenizer[1]);
        context.write(new LongWritable(Long.parseLong(vertexIn.toString())), new Text(vertexOut));
        
		
	}
       
       
      
 }//Class MAP
 
 
 

