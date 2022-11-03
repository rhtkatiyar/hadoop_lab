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

public  class GisReduce extends Reducer<LongWritable, Text, Text, Text>
{       
      int k;
      String userrecord[];
      public void setup(Context context) throws IOException, InterruptedException
      {
        userrecord=context.getConfiguration().get("userInput").split("\n");
        k=Integer.parseInt(context.getConfiguration().get("k"));
      }
      
		 //TreeSet<int,long> heap = new TreeSet<int,long>();
		 //Context context = null;
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
				String []dis_record=record[2].split(":");
				String []dis_userrecord=userrecord[2].split(":");
				
				long Lxi=Long.parseLong(dis_record[0]);
				long Lyi=Long.parseLong(dis_record[1]);
				long Lxj=Long.parseLong(dis_userrecord[0]);
				long Lyj=Long.parseLong(dis_userrecord[1]);
				int normalize=0;
				
				if(record[2]!=""&&userrecord[2]!="")
				normalize=normalize+1;
				if(record[3]!=""&&userrecord[3]!="")
				normalize=normalize+1;
				if(record[4]!=""&&userrecord[4]!="")
				normalize=normalize+1;
				if(record[5]!=""&&userrecord[5]!="")
				normalize=normalize+1;
				if(record[6]!=""&&userrecord[6]!="")
				normalize=normalize+1;
				if(record[7]!=""&&userrecord[7]!="")
				normalize=normalize+1;
				if(record[8]!=""&&userrecord[8]!="")
				normalize=normalize+1;
				if(record[9]!=""&&userrecord[9]!="")
				normalize=normalize+1;
				if(record[10]!=""&&userrecord[10]!="")
				normalize=normalize+1;
				if(record[11]!=""&&userrecord[11]!="")
				normalize=normalize+1;
				if(record[12]!=""&&userrecord[12]!="")
				normalize=normalize+1;
				if(record[13]!=""&&userrecord[13]!="")
				normalize=normalize+1;
				if(record[14]!=""&&userrecord[14]!="")
				normalize=normalize+1;
				if(normalize==0)
				normalize=1;
				
				long similarity=(Math.abs(Lxi-Lxj)+Math.abs(Lyi-Lyj))/normalize;
				
				this.heap.add(new TopKRecord(new Text(record[1]), similarity));
				if (this.heap.size() >= k) {
				TopKRecord removed = this.heap.pollLast();
				if (removed == null)
					throw new IllegalStateException();
			}
				
				
				//context.write(new Text(t.toString().split("\t")[0]),new Text(t.toString().split("\t")[1]));
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
