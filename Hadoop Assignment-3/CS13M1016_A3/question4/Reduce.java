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

       

public class Reduce  extends Reducer<LongWritable, Text, LongWritable,Text>
{
	long p;
	long[ ][ ] ab;
	int s1,c1;
	
	public void setup(Context context) throws IOException,InterruptedException
		{
			Configuration conf = context.getConfiguration();
			s1 = Integer.parseInt(conf.get("s1"));
			c1=Integer.parseInt(conf.get("c1"));
			BigInteger bi;
			int bitLength = 40;
			ab = new long[c1][2];
			Random rnd = new Random();
			bi = BigInteger.probablePrime(bitLength, rnd);
			p=bi.longValue();
			
			for(int i=0;i<c1;i++)
			{
				ab[i][0]=Math.abs((rnd.nextLong()))%p;
				ab[i][1]=Math.abs((rnd.nextLong()))%p;
			}
			
		}
	
	
	
	
     public void reduce(LongWritable key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
		{ 
			
			ArrayList <Long> outlinks =new ArrayList <Long>();
			String line=values.toString();
			//StringTokenizer tokenizer = new StringTokenizer(line);
			//LongWritable vertexIn=(int)(key.get());
			long Hiu=0;
			//String temp;
			
			
			//while(tokenizer.hasMoreTokens())
			//{
			  for (Text value : values)
				{
					String temp=value.toString();
					String[] tokenizer = temp.split("	");
					for(int k=0;k<tokenizer.length;k++)
					outlinks.add(Long.parseLong(tokenizer[k]));
				}
			//}
			String zs="";
			for(int j=0 ;j<c1 ;j++)
			{
				
				TreeSet<Long> maxheap = new TreeSet<Long>();
				for(int i=0;i<outlinks.size();i++)
				{
					Hiu=Math.abs((ab[j][0]*outlinks.get(i)+ab[j][1]))%p;
					maxheap.add(Hiu);
					if(maxheap.size()>s1)
					{
						long extractmax=maxheap.pollLast();
					}
				}
				
				//String ys="";
				long shingleZ=0;
				int pow=0;
				for(long val:maxheap)
				{
					/*if(ys=="")
					ys=ys+Long.toString(val);
					else
					ys=ys+"	"+Long.toString(val);*/
					
					shingleZ+=(val*Math.pow(10,pow));
					pow++;
					
				}
				shingleZ=Math.abs(shingleZ)%p;
				
				if(zs=="")
				zs=zs+Long.toString(shingleZ);
				else
				zs=zs+"	"+Long.toString(shingleZ);
				//context.write(key, new Text(Long.toString(shingleZ)));
				
			}
			//context.write(key, new Text(Long.toString(shingleZ)));
			context.write(key, new Text(zs));
			////test print
			/*String outlink="";
			for (Text value : values)
			{
				if(outlink=="")
				{
					outlink+=value.toString();
				}
				else
				{
					outlink=value.toString()+"	"+outlink;         
				}
			}
      
			context.write(key, new Text(outlink));*/
			////end test print
			
			
					
			                     
		}
				

}	

