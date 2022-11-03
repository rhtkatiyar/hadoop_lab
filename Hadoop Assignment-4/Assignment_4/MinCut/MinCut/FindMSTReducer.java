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
				//boolean values to check if the two nodes belong to the same tree, useful for cycle detection
				boolean ignoreEdgeSameSet1 = false;
				boolean ignoreEdgeSameSet2 = false;
				boolean ignoreEdgeSameSet3 = false;
				Set<String> nodesSet = new HashSet<String>();
				//check if src and dest belong to the same tree/set, if so, ignore the edge
				ignoreEdgeSameSet1 = isSameSet(src, dest);
				//form the verticesSet
				nodesSet.add(src);
				nodesSet.add(dest);			
				ignoreEdgeSameSet2 = unionSet(nodesSet, src, dest);				
				ignoreEdgeSameSet3 = unionSet(nodesSet, dest, src);
				if(!ignoreEdgeSameSet1 && !ignoreEdgeSameSet2 && !ignoreEdgeSameSet3)
				{
					return true;
				}
				else
				{
					return false;
				}
			
		}

		//method to unite the set of the two nodes - node1 and node2, this is useful to add edges to the tree without forming cycles
	private boolean unionSet(Set<String> nodesSet, String node1, String node2)
	{
		boolean ignoreEdge = false;//boolean value to determine whether to ignore the edge
		//if the map does not contain the key, add the key, value pair
		if (!node_AssociatedSet.containsKey(node1))
		{
			node_AssociatedSet.put(node1, nodesSet);
		 }
		 else
		 {
				// get the set associated with the key
				Set<String> associatedSet = node_AssociatedSet.get(node1);
				Set<String > nodeSet = new HashSet<String>();
				nodeSet.addAll(associatedSet); 
				Iterator<String> nodeItr = nodeSet.iterator();
				Iterator<String> duplicateCheckItr = nodeSet.iterator();
				
				
				//first check if the second node is contained in any of the sets from node1 to nodeN
				// if so, ignore the edge as the two nodes belong to the same set/tree
				while(duplicateCheckItr.hasNext()){
					
					String n = duplicateCheckItr.next();
					if(node_AssociatedSet.get(n).contains(node2)){
					 ignoreEdge =  true;
					}
				}
				
				
				//if the associatedSet contains elements {node1 , node2, .., nodeN}
				//get the sets associated with each of the element from node1 to nodeN
				while (nodeItr.hasNext()) {

					String nextNode = nodeItr.next();
					
					if (!node_AssociatedSet.containsKey(nextNode)) {
						
						node_AssociatedSet.put(nextNode, nodesSet);
					}
					//add the src and dest to the set associated with each of the elements in the associatedSet
					//the src and dest will get added to the set associated with node1 to nodeN
					node_AssociatedSet.get(nextNode).addAll(nodesSet);

				}
			}
			return ignoreEdge;
			
		}
		
		
		//method to determine if the two nodes belong to the same set
		//this is done by iterating through the map and checking if any of the set contains the two nodes

		private boolean isSameSet(String src, String dest)
		{
			boolean ignoreEdge = false; //boolean value to check whether the edge should be ignored
			
			//iterating through the map
			for (Map.Entry<String, Set<String>> node_AssociatedSetValue : node_AssociatedSet.entrySet())
			{
				Set<String> nodesInSameSet = node_AssociatedSetValue.getValue();
				//if the src and dest of an edge are in the same set, ignore the edge
				if (nodesInSameSet.contains(src)&& nodesInSameSet.contains(dest))
				{
					ignoreEdge= true;
				 }

			}

			return ignoreEdge;
		}	
		
	}
