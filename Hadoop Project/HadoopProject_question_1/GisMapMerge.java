import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class GisMapMerge extends Mapper<LongWritable,Text, LongWritable,Text>
{
	  long recordid[];
      public void setup(Context context) throws IOException, InterruptedException
      {
        recordid=new long[Integer.parseInt(context.getConfiguration().get("records"))];
        String lines[]=context.getConfiguration().get("combineoutput").split("\n");
        for(int i=0;i<Integer.parseInt(context.getConfiguration().get("records"));i++)
        {
			recordid[i]=0;
		}
        for(String line:lines)
        {
            recordid[Integer.parseInt(line.split("\t")[0])-1]=Integer.parseInt(line.split("\t")[1]);
        }
      }
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
			
            String record[]=value.toString().split("\t");
            String redid=record[0];
            if(recordid[Integer.parseInt(record[0])-1]!=0)
            context.write(new LongWritable(Long.parseLong(redid.toString())),new Text(""+recordid[Integer.parseInt(record[0])-1]+value));
        }
        
}
