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
       

public class ReduceCC  extends Reducer<LongWritable,Text, LongWritable,Text>
{
	Pseudograph <Long,DefaultEdge> g=new Pseudograph<Long,DefaultEdge>(DefaultEdge.class);
	List<Set<Long>> listofsets=new ArrayList<Set<Long>>();
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
					int i=1;
					ConnectivityInspector ci=new ConnectivityInspector(g);
					listofsets=ci.connectedSets() ; 
					Iterator<Set<Long>> iterator =listofsets.iterator();
					while(iterator.hasNext())
					{					
						Set<Long> se=iterator.next();
						Iterator itr =se.iterator();
						while(itr.hasNext())
						{
						String str= ""+itr.next();
						context.write(new LongWritable(Long.parseLong(str)),new Text(""+i));
						}
					i++;
					}
					
				}

}	

