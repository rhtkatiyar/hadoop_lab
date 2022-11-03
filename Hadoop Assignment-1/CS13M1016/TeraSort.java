import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import java.io.DataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 class TeraInputFormat extends FileInputFormat<Text,Text> {

  static final String PARTITION_FILENAME = "_partition.lst";
  static final String SAMPLE_SIZE = "terasort.partitions.sample";
  private static JobConf lastConf = null;
  private static InputSplit[] lastResult = null;

  static class TextSampler implements IndexedSortable {
    private ArrayList<Text> records = new ArrayList<Text>();

    public int compare(int i, int j) {
      Text left = records.get(i);
      Text right = records.get(j);
      return left.compareTo(right);
    }

    public void swap(int i, int j) {
      Text left = records.get(i);
      Text right = records.get(j);
      records.set(j, left);
      records.set(i, right);
    }

    public void addKey(Text key) {
      records.add(new Text(key));
    }

    Text[] createPartitions(int numPartitions) {
      int numRecords = records.size();
      System.out.println("Making " + numPartitions + " from " + numRecords + 
                         " records");
      if (numPartitions > numRecords) {
        throw new IllegalArgumentException
          ("Requested more partitions than input keys (" + numPartitions +
           " > " + numRecords + ")");
      }
      new QuickSort().sort(this, 0, records.size());
      float stepSize = numRecords / (float) numPartitions;
      System.out.println("Step size is " + stepSize);
      Text[] result = new Text[numPartitions-1];
      for(int i=1; i < numPartitions; ++i) {
        result[i-1] = records.get(Math.round(stepSize * i));
      }
      return result;
    }
  }
  
 
  public static void writePartitionFile(JobConf conf, 
                                        Path partFile) throws IOException {
    TeraInputFormat inFormat = new TeraInputFormat();
    TextSampler sampler = new TextSampler();
    Text key = new Text();
    Text value = new Text();
    int partitions = conf.getNumReduceTasks();
    long sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
    InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
    int samples = Math.min(10, splits.length);
    long recordsPerSample = sampleSize / samples;
    int sampleStep = splits.length / samples;
    long records = 0;
    // take N samples from different parts of the input
    for(int i=0; i < samples; ++i) {
      RecordReader<Text,Text> reader = 
        inFormat.getRecordReader(splits[sampleStep * i], conf, null);
      while (reader.next(key, value)) {
        sampler.addKey(key);
        records += 1;
        if ((i+1) * recordsPerSample <= records) {
          break;
        }
      }
    }
    FileSystem outFs = partFile.getFileSystem(conf);
    if (outFs.exists(partFile)) {
      outFs.delete(partFile, false);
    }
    SequenceFile.Writer writer = 
      SequenceFile.createWriter(outFs, conf, partFile, Text.class, 
                                NullWritable.class);
    NullWritable nullValue = NullWritable.get();
    for(Text split : sampler.createPartitions(partitions)) {
      writer.append(split, nullValue);
    }
    writer.close();
  }

  static class TeraRecordReader implements RecordReader<Text,Text> {
    private LineRecordReader in;
    private LongWritable junk = new LongWritable();
    private Text line = new Text();
    private static int KEY_LENGTH = 10;

    public TeraRecordReader(Configuration job,FileSplit split) throws IOException {
      in = new LineRecordReader(job, split);
    }

    public void close() throws IOException {
      in.close();
    }

    public Text createKey() {
      return new Text();
    }

    public Text createValue() {
      return new Text();
    }

    public long getPos() throws IOException {
      return in.getPos();
    }

    public float getProgress() throws IOException {
      return in.getProgress();
    }

    public boolean next(Text key, Text value) throws IOException {
      if (in.next(junk, line)) {
        if (line.getLength() < KEY_LENGTH) {
          key.set(line);
          value.clear();
        } else {
          byte[] bytes = line.getBytes();
          key.set(bytes, 0, KEY_LENGTH);
          value.set(bytes, KEY_LENGTH, line.getLength() - KEY_LENGTH);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public RecordReader<Text, Text> getRecordReader(InputSplit split,JobConf job,Reporter reporter) throws IOException {
    return new TeraRecordReader(job, (FileSplit) split);
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
    if (conf == lastConf) {
      return lastResult;
    }
    lastConf = conf;
    lastResult = super.getSplits(conf, splits);
    return lastResult;
  }
}

 class TeraOutputFormat extends TextOutputFormat<Text,Text> {
  static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

  public static void setFinalSync(JobConf conf, boolean newValue) {
    conf.setBoolean(FINAL_SYNC_ATTRIBUTE, newValue);
  }

  public static boolean getFinalSync(JobConf conf) {
    return conf.getBoolean(FINAL_SYNC_ATTRIBUTE, false);
  }

  static class TeraRecordWriter extends LineRecordWriter<Text,Text> {
    private static final byte[] newLine = "\r\n".getBytes();
    private boolean finalSync = false;

    public TeraRecordWriter(DataOutputStream out,
                            JobConf conf) {
      super(out);
      finalSync = getFinalSync(conf);
    }

    public synchronized void write(Text key, 
                                   Text value) throws IOException {
      out.write(key.getBytes(), 0, key.getLength());
      out.write(value.getBytes(), 0, value.getLength());
      out.write(newLine, 0, newLine.length);
    }
    
    public void close() throws IOException {
      if (finalSync) {
        ((FSDataOutputStream) out).sync();
      }
      super.close(null);
    }
  }

  public RecordWriter<Text,Text> getRecordWriter(FileSystem ignored,JobConf job,String name,Progressable progress) throws IOException {
    Path dir = getWorkOutputPath(job);
    FileSystem fs = dir.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
    return new TeraRecordWriter(fileOut, job);
  }
}

public class TeraSort extends Configured implements Tool {
  static class TotalOrderPartitioner implements Partitioner<Text,Text>{
    private TrieNode trie;
    private Text[] splitPoints;

    static abstract class TrieNode {
      private int level;
      TrieNode(int level) {
        this.level = level;
      }
      abstract int findPartition(Text key);
      abstract void print(PrintStream strm) throws IOException;
      int getLevel() {
        return level;
      }
    }

    static class InnerTrieNode extends TrieNode {
      private TrieNode[] child = new TrieNode[256];
      
      InnerTrieNode(int level) {
        super(level);
      }
      int findPartition(Text key) {
        int level = getLevel();
        if (key.getLength() <= level) {
          return child[0].findPartition(key);
        }
        return child[key.getBytes()[level]].findPartition(key);
      }
      void setChild(int idx, TrieNode child) {
        this.child[idx] = child;
      }
      void print(PrintStream strm) throws IOException {
        for(int ch=0; ch < 255; ++ch) {
          for(int i = 0; i < 2*getLevel(); ++i) {
            strm.print(' ');
          }
          strm.print(ch);
          strm.println(" ->");
          if (child[ch] != null) {
            child[ch].print(strm);
          }
        }
      }
    }


    static class LeafTrieNode extends TrieNode {
      int lower;
      int upper;
      Text[] splitPoints;
      LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
        super(level);
        this.splitPoints = splitPoints;
        this.lower = lower;
        this.upper = upper;
      }
      int findPartition(Text key) {
        for(int i=lower; i<upper; ++i) {
          if (splitPoints[i].compareTo(key) >= 0) {
            return i;
          }
        }
        return upper;
      }
      void print(PrintStream strm) throws IOException {
        for(int i = 0; i < 2*getLevel(); ++i) {
          strm.print(' ');
        }
        strm.print(lower);
        strm.print(", ");
        strm.println(upper);
      }
    }



    private static Text[] readPartitions(FileSystem fs, Path p, 
                                         JobConf job) throws IOException {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, job);
      List<Text> parts = new ArrayList<Text>();
      Text key = new Text();
      NullWritable value = NullWritable.get();
      while (reader.next(key, value)) {
        parts.add(key);
        key = new Text();
      }
      reader.close();
      return parts.toArray(new Text[parts.size()]);  
    }

    /**
     * Given a sorted set of cut points, build a trie that will find the correct
     * partition quickly.
     * @param splits the list of cut points
     * @param lower the lower bound of partitions 0..numPartitions-1
     * @param upper the upper bound of partitions 0..numPartitions-1
     * @param prefix the prefix that we have already checked against
     * @param maxDepth the maximum depth we will build a trie for
     * @return the trie node that will divide the splits correctly
     */
    private static TrieNode buildTrie(Text[] splits, int lower, int upper, 
                                      Text prefix, int maxDepth) {
      int depth = prefix.getLength();
      if (depth >= maxDepth || lower == upper) {
        return new LeafTrieNode(depth, splits, lower, upper);
      }
      InnerTrieNode result = new InnerTrieNode(depth);
      Text trial = new Text(prefix);
      // append an extra byte on to the prefix
      trial.append(new byte[1], 0, 1);
      int currentBound = lower;
      for(int ch = 0; ch < 255; ++ch) {
        trial.getBytes()[depth] = (byte) (ch + 1);
        lower = currentBound;
        while (currentBound < upper) {
          if (splits[currentBound].compareTo(trial) >= 0) {
            break;
          }
          currentBound += 1;
        }
        trial.getBytes()[depth] = (byte) ch;
        result.child[ch] = buildTrie(splits, lower, currentBound, trial, 
                                     maxDepth);
      }
      // pick up the rest
      trial.getBytes()[depth] = 127;
      result.child[255] = buildTrie(splits, currentBound, upper, trial,
                                    maxDepth);
      return result;
    }

    public void configure(JobConf job) {
      try {
        FileSystem fs = FileSystem.getLocal(job);
        Path partFile = new Path(TeraInputFormat.PARTITION_FILENAME);
        splitPoints = readPartitions(fs, partFile, job);
        trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
      } catch (IOException ie) {
        throw new IllegalArgumentException("can't read paritions file", ie);
      }
    }

    public TotalOrderPartitioner() {
    }

    public int getPartition(Text key, Text value, int numPartitions) {
      return trie.findPartition(key);
    }
    
  }
  
  public int run(String[] args) throws Exception {
    JobConf job = (JobConf) getConf();
    Path inputDir = new Path(args[0]);
    inputDir = inputDir.makeQualified(inputDir.getFileSystem(job));
    Path partitionFile = new Path(inputDir, TeraInputFormat.PARTITION_FILENAME);
    URI partitionUri = new URI(partitionFile.toString() +
                               "#" + TeraInputFormat.PARTITION_FILENAME);
    TeraInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJobName("TeraSort");
    job.setJarByClass(TeraSort.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormat(TeraInputFormat.class);
    job.setOutputFormat(TeraOutputFormat.class);
    job.setPartitionerClass(TotalOrderPartitioner.class);
    TeraInputFormat.writePartitionFile(job, partitionFile);
    DistributedCache.addCacheFile(partitionUri, job);
    DistributedCache.createSymlink(job);
    job.setInt("dfs.replication", 1);
    TeraOutputFormat.setFinalSync(job, true);
    JobClient.runJob(job);
    return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JobConf(), new TeraSort(), args);
    System.exit(res);
  }

}
