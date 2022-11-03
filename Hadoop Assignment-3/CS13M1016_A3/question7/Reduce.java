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

       

public class Reduce  extends Reducer<Text, Text, Text,Text>
{
	   public void reduce(Text key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
		{ 
			
			String outlink="";
			for (Text value : values)
			{
				if(outlink=="")
				outlink+=value.toString();
				else
				outlink=outlink+"	"+value.toString();         
			 }
			 String tempkey=key.toString();
			 String[] check=outlink.split("	");
			 
			 int flagd=0;
			 
			 for(int k=0 ;k<check.length;k++)
			 {
				 if(tempkey.equals(check[k]))
				 {
					 flagd=1;
				 }
				 else
				 {
					flagd=0;
					break;
				 }
			 }

			 
      		if(flagd==1)
      		{
			context.write(new Text(key), new Text(check[0]));
			}
		               
			else
			{
			context.getCounter(main.loop.stillleft).increment(1);	
			HashSet<Long> shingleset = new HashSet<Long>();
			TreeSet<Long> unionset =new TreeSet<Long>();
            String line=outlink.toString();
			String[] tokenizer=line.split("	");
			String keymake="";
			for(int k=0;k<tokenizer.length;k++)
				{
					String[] commasplit=tokenizer[k].split(",");
					for(int i=0;i<commasplit.length;i++)
					{
						shingleset.add(Long.parseLong(commasplit[i]));
						unionset.add(Long.parseLong(commasplit[i])); 
					}
					
					String key1="";
					for(long val:unionset)
					{
						if(key1=="")
						key1=key1+ Long.toString(val);
						else
						key1=key1+","+Long.toString(val);
					}
					unionset.clear();
					if(keymake=="")
					keymake=keymake+key1;
					else
					keymake=keymake+"	"+key1;
				}
				String keyval="";
				unionset.addAll(shingleset);
				for(long val:unionset)
				{
					if(keyval=="")
					keyval=keyval+ Long.toString(val);
					else
					keyval=keyval+","+Long.toString(val);
				}
				String [] temp=keymake.split("	");
				for(int i=0;i<temp.length;i++)
				context.write(new Text(temp[i]), new Text(keyval));
			}
			
			
		}
		
}	

