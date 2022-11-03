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

public class FindMSTReducer extends Reducer<IntWritable, Text, Text, Text>
{
	Map<String, Set<String>> node_AssociatedSet = new HashMap<String, Set<String>>(); 
	public void reduce(IntWritable inputKey, Iterable<Text> values, Context context)throws IOException, InterruptedException
	{
		String strKey = new String();
		strKey += inputKey;
		Text outputKey = new Text(strKey);
		for (Text val : values)
		{
			String[] srcDest = val.toString().split(":");
			String src = srcDest[1]; 
			String dest = srcDest[2];
			if (mstFind(src,dest))
			{
				long weight = Long.parseLong(outputKey.toString());
				context.getCounter(main.MSTCounters.totalWeight).increment(weight);
				String[] vertexInOut = val.toString().split(":");
				String vertexIn = vertexInOut[1]; 
				String vertexOut = vertexInOut[2];
				String redId=vertexInOut[0];
				String temp=vertexOut+"\t"+outputKey;
				context.write(new Text(vertexIn), new Text(temp));
			}
		}

	}
		
	private boolean mstFind(String src , String dest)
	{
		boolean ignoreEdgeSameSet1 = false;
		boolean ignoreEdgeSameSet2 = false;
		boolean ignoreEdgeSameSet3 = false;
		Set<String> nodesSet = new HashSet<String>();
		ignoreEdgeSameSet1 = checkSameSet(src, dest);
		nodesSet.add(src);
		nodesSet.add(dest);			
		ignoreEdgeSameSet2 = takeunion(nodesSet, src, dest);				
		ignoreEdgeSameSet3 = takeunion(nodesSet, dest, src);
		if(!ignoreEdgeSameSet1 && !ignoreEdgeSameSet2 && !ignoreEdgeSameSet3)
		{
			return true;
		}
		else
		{
			return false;
		}
			
	}

	
	private boolean takeunion(Set<String> nodesSet, String node1, String node2)
	{
		boolean ignoreEdge = false;
		if (!node_AssociatedSet.containsKey(node1))
		{
			node_AssociatedSet.put(node1, nodesSet);
		}
		 else
		 {
			Set<String> associatedSet = node_AssociatedSet.get(node1);
			Set<String > nodeSet = new HashSet<String>();
			nodeSet.addAll(associatedSet); 
			Iterator<String> nodeItr = nodeSet.iterator();
			Iterator<String> duplicateCheckItr = nodeSet.iterator();
		
			while(duplicateCheckItr.hasNext())
			{
				String n = duplicateCheckItr.next();
				if(node_AssociatedSet.get(n).contains(node2))
				{
				ignoreEdge =  true;
				}
			}

			while (nodeItr.hasNext())
			{
				String nextNode = nodeItr.next();
				if (!node_AssociatedSet.containsKey(nextNode))
				{
					node_AssociatedSet.put(nextNode, nodesSet);
				}
				node_AssociatedSet.get(nextNode).addAll(nodesSet);

			}
		}
		return ignoreEdge;
			
	}
		
	private boolean checkSameSet(String src, String dest)
	{
		boolean ignoreEdge = false;
		
		for (Map.Entry<String, Set<String>> node_AssociatedSetValue : node_AssociatedSet.entrySet())
		{
			Set<String> nodesInSameSet = node_AssociatedSetValue.getValue();
			if (nodesInSameSet.contains(src)&& nodesInSameSet.contains(dest))
			{
				ignoreEdge= true;
			}
		}
		return ignoreEdge;
	}	
		
}
