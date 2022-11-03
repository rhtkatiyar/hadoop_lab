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

       

public class Reduce4  extends Reducer<Text, Text, Text,Text>
{	
	 ArrayList <String> mapper8 ;	
	 HashMap <String,String> mapper4 ;
	 
	 public void setup(Context context)
	 {
		 mapper8=new ArrayList <String>();
		 mapper4=new HashMap<String,String>();
	 }
     public void reduce(Text key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
		{ 
					
			for (Text value:values)
			{
				String line=value.toString();
				String[] tokenizer=line.split(",");
				//context.write(new Text(tokenizer[0]),new Text(line));
				
				if(tokenizer[0].equals("UniqueId"))
				{
					mapper8.add(line);
					//context.write(new Text(tokenizer[0]),new Text(line));
				}
				if(tokenizer[0].equals("PrimaryId"))
				{
					
						mapper4.put(tokenizer[1],line);
					//	context.write(new Text(tokenizer[0]),new Text(line));
					
				}
					
				}

			}
		public void cleanup(Context context)	throws IOException ,InterruptedException 
		{
			for (String val:mapper8)
			{
				String[] temp =val.split(",");
				for(int k=1;k<temp.length;k++)
				{
					String retur=mapper4.get(temp[k]);
					context.write(new Text(temp[k]),new Text(retur));
				}
				context.write(new Text("***********************"),new Text("*********************************"));
			}
			
		}
			                     
}
				



