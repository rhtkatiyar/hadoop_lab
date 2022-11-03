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

public class GisMap extends Mapper<LongWritable,Text, LongWritable,Text>
{
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
			if(value.toString().length()==16)
            {String record[]=value.toString().split("\t");
            String key1=record[1];
            context.write(new LongWritable(Long.parseLong(key1)),new Text(value));}
        }
        
}
