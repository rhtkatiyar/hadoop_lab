import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountK extends Configured implements Tool {

	public static class Map<K> extends MapReduceBase
	implements Mapper<K, Text, Text, LongWritable> {

	    private final static LongWritable one = new LongWritable(1);
	    private Text word = new Text();

	

		public void map(K key, Text value,
				OutputCollector<Text, LongWritable> output,
				Reporter reporter)
		throws IOException {
			String line = value.toString();
		    StringTokenizer itr = new StringTokenizer(line);

		    while (itr.hasMoreTokens()) {
		    	word.set(itr.nextToken());
		    	output.collect(word, one);
		    }
		}
	}



	public static class Combiner<K> extends MapReduceBase
    	implements Reducer<K, LongWritable, K, LongWritable> {

 	 public void reduce(K key, Iterator<LongWritable> values,OutputCollector<K, LongWritable> output,Reporter reporter)
   	 throws IOException {

    
    	long sum = 0;
    	while (values.hasNext()) {
     	 sum += values.next().get();
    	}

       	output.collect(key, new LongWritable(sum));
  	}

}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {

		private static class TopKRecord implements Comparable<TopKRecord> {
			public Text key;
			public long sum;

			public TopKRecord(Text key, long sum) {
				this.key = key;
				this.sum = sum;
			}

			
			public boolean equals(Object obj) {
				if (!(obj instanceof TopKRecord))
					return false;

				TopKRecord second = (TopKRecord) obj;
				if (second.sum == this.sum && second.key.equals(this.key))
					return true;
				else
					return false;
			}

		

		
			public int compareTo(TopKRecord other) {
				if (this.sum == other.sum)
					return this.key.compareTo(other.key);

				return (this.sum < other.sum ? -1 : 1);
			}
		}

		private final Queue<TopKRecord> minPriorityheap = new PriorityQueue<TopKRecord>();
		private OutputCollector<Text, LongWritable> target = null;

		int k = 0;

		@Override
		public void configure(JobConf job) {
			k = job.getInt("topk", 1);
		}

		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			
			if (target == null)
				target = output;

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			
			this.minPriorityheap.add(new TopKRecord(new Text(key), sum));
			if (this.minPriorityheap.size() > k) {
				TopKRecord removed = this.minPriorityheap.poll();
				if (removed == null)
					throw new IllegalStateException();
			}
		}

		public void close() throws IOException {
			if (this.target == null) {
				assert(this.minPriorityheap.size() == 0);
				return;
			}
			
			TreeSet<TopKRecord> treesetr=new TreeSet<TopKRecord>();					
			
			for(TopKRecord rec : this.minPriorityheap){
				treesetr.add(rec);
			}	

			for (TopKRecord rec : treesetr) {
				this.target.collect(rec.key, new LongWritable(rec.sum));
			}

			this.minPriorityheap.clear();
			this.target = null;
		}
	}

	
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordCountK.class);
		conf.setJobName("wordcountk");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setNumReduceTasks(1);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combiner.class);
		conf.setReducerClass(Reduce.class);
		FileInputFormat.setInputPaths(conf, args[1]);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		conf.setInt("topk", Integer.parseInt(args[3]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountK(), args);
		System.exit(res);
	}
}

