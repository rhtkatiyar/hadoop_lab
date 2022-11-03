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

public class MapEdges  {
       
    public static class EdgeMap extends Mapper<LongWritable,Text, LongWritable,Text>{
        //input graph file
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
            int r=Integer.parseInt(context.getConfiguration().get("r"));
            String edge[]=value.toString().split("\t");
            int h=(Integer.parseInt(edge[0])%r+Integer.parseInt(edge[1])%r)%r;
            //System.out.println(edge[0]+","+edge[1]+" -> "+h);
            context.write(new LongWritable(h),new Text(value));
        }
    }
   
    public static class MyPartitioner extends Partitioner<LongWritable,Text>{
        public int getPartition(LongWritable key, Text value, int numPartitions){
                return (int)key.get();
        }
    }
   
    public static class EdgeReduce extends Reducer<LongWritable, Text, Text, Text>{       
      int cid[];
      public void setup(Context context) throws IOException, InterruptedException{
        cid=new int[Integer.parseInt(context.getConfiguration().get("v"))];
        for(int i=0;i<Integer.parseInt(context.getConfiguration().get("v"));i++)
        cid[i]=2;
        String lines[]=context.getConfiguration().get("componentIds").split("\n");
        for(String line:lines)
        {	
			String [] str=line.split("\t");
			for(int i=1;i<str.length;i++)
			{
            cid[Integer.parseInt(str[i])-1]=1;
			}
        }
      }
    
     public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text t:values){
                if(cid[Integer.parseInt(t.toString().split("\t")[0])-1]!=cid[Integer.parseInt(t.toString().split("\t")[1])-1]){
                    context.write(new Text(t.toString().split("\t")[0]),new Text(t.toString().split("\t")[1]));
                }
            }
     }
    }
     
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("r",args[0]);
        conf.set("v",args[1]);
       
        BufferedReader br=new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("/rohit/com/map4"))));
        String line="",componentIds="";
        while((line=br.readLine())!=null)
        componentIds = componentIds+line+"\n";
        conf.set("componentIds",componentIds);
        Job job =new Job(conf);
        job.setJarByClass(MapEdges.class);
        job.setMapperClass(EdgeMap.class);
        job.setReducerClass(EdgeReduce.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(Integer.parseInt(args[0]));
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path("/rohit/graph"));
        FileOutputFormat.setOutputPath(job,new Path("/rohit/cc"));
        job.waitForCompletion(true);
    }     
}
