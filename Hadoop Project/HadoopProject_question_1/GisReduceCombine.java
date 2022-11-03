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

public  class GisReduceCombine extends Reducer<LongWritable, Text, Text, Text>
{       
      int k;
      String userrecord[];
      int topk;
      int numclass;
      public void setup(Context context) throws IOException, InterruptedException
      {
         k=Integer.parseInt(context.getConfiguration().get("k"));
         numclass=Integer.parseInt(context.getConfiguration().get("numclass"));
         topk=numclass*k/2;
      }

	private static class TopKRecord implements Comparable<TopKRecord> {
			public Text key;
			public long sum;

			public TopKRecord(Text key, long sum) {
				this.key = key;
				this.sum = sum;
			}

			@Override
			public boolean equals(Object obj) {
				if (!(obj instanceof TopKRecord))
					return false;

				TopKRecord other = (TopKRecord) obj;
				if (other.sum == this.sum && other.key.equals(this.key))
					return true;
				else
					return false;
			}

			@Override
			public int hashCode() {
				return this.key.hashCode() ^ new Long(this.sum).intValue();
			}

			@Override
			public int compareTo(TopKRecord other) {
				if (this.sum == other.sum)
					return this.key.compareTo(other.key);

				return (this.sum < other.sum ? -1 : 1);
			}
		}
		
		TreeSet<TopKRecord> heap = new TreeSet<TopKRecord>();
     public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
     {
		        for(Text t:values)
				{
					String []record=values.toString().split("\t");
					long similarity=Long.parseLong(record[1]);
					this.heap.add(new TopKRecord(new Text(record[0]), similarity));
					if (this.heap.size() >= topk)
					{
						TopKRecord removed = this.heap.pollLast();
						if (removed == null)
						throw new IllegalStateException();
					}
				}
     }
		public void cleanup(Context context)	throws IOException ,InterruptedException 
		{
			for (TopKRecord rec : this.heap)
			{
				context.write(new Text(rec.key+""), new Text(rec.sum+""));
			}
			
		}
}
