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
import org.jgrapht.graph.*;
public class MapperMinCut extends Mapper<LongWritable, Text, LongWritable,Text>
{
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException
		{
			String []inputTokens = value.toString().split("\t");  
			String vertexIn=inputTokens[0];
			String vertexOut=inputTokens[1];
		    context.write(new LongWritable(Long.parseLong(vertexIn.toString())),new Text(vertexIn+"\t"+vertexOut));
		}
}

