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
import org.jgrapht.alg.*;
       

public class ReducerMinCut  extends Reducer<LongWritable,Text, LongWritable,Text>
{
	Pseudograph <Long,DefaultEdge> g=new Pseudograph<Long,DefaultEdge>(DefaultEdge.class);
	Set<Long> listofsets;
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
	{ 
		for(Text val:values)
		{	
			String[] str = val.toString().split("\t");
			g.addVertex(Long.parseLong(str[0]));
			g.addVertex(Long.parseLong(str[1]));
			g.addEdge(Long.parseLong(str[0]),Long.parseLong(str[1]));
		}
	}
	protected void cleanup(Context context) throws IOException,InterruptedException
				{
					int i=0;
					StoerWagnerMinimumCut ci=new StoerWagnerMinimumCut(g);
					listofsets=ci.minCut(); 
					Iterator<Long> iterator =listofsets.iterator();
					String txt="";
					while(iterator.hasNext())
					{
						String se=""+iterator.next();
						if(txt=="")
						txt=txt+se;
						else
						{
						txt=txt+"\t"+se;
						}
					}
					context.write(new LongWritable((int)i), new Text(txt));
					
					}
					
}



