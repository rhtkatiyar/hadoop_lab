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


        
public class kLengthPath {
            
 public static class Map extends Mapper<LongWritable, Text , LongWritable, Text> {
    
    private Text hkey1 = new Text();
    private Text hkey2 = new Text();
    private Text x = new Text();
    private Text y = new Text();
   	public  int k ,num_of_reducer;
	public  int	 m,size,n;	
	public  int  evenrelationnum,oddrelationnum;
	int iter = 0;
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
        String line = value.toString();
        Configuration conf = context.getConfiguration();
		k =Integer.parseInt(conf.get("k"));
		m =Integer.parseInt(conf.get("m"));
		int mpowkby2 =(int)(Math.pow(m,Math.ceil(k/2)));
		num_of_reducer = mpowkby2;
        StringTokenizer tokenizer = new StringTokenizer(line);
        hkey1.set(tokenizer.nextToken());
        hkey2.set(tokenizer.nextToken());
        evenrelationnum=1;
        oddrelationnum=1;
	
	if(k%2 == 0)//Even Path
		{
			for(int relid = 1 ;relid <= k ; relid++)	
			{				
				if(relid%2 == 1)
				{
					x = hkey2;
					y = hkey1;	
					evenrelationnum++;					
				}				
				if(relid%2 == 0)
				{
					x = hkey1;
					y = hkey2;
					
				}
				
					size = m+1;
					n =(int)Math.ceil((double)(k)/2);
					int[] coordinate = new int[n];
					int p=1;
					
					for(int l=n-1;l>=0&&p>0;p++)
					{
						
						if((l == n - 1) && (coordinate[evenrelationnum-2] == (Integer.parseInt(x.toString()) % m))) {
						int rednum= 0;
						
						for(int axis = 0 ; axis < n ; axis++)
						{
							rednum += (coordinate[axis] * Math.pow(m,axis));
						}
						String redid = Integer.toString(rednum);
						context.write(new LongWritable(Long.parseLong(x.toString())), new Text(relid +" " + redid + " "+ y));
						}
						coordinate[l]++;
						if(coordinate[l] == size - 1)
						{
							coordinate[l] = 0;
							l--;
						 }
						 else if(l < n - 1)
						 {
						   while(l<n-1)
							l++;
						 }
					}//for(int l=n-1;l>=0&&p>0;p++)
			}//for(int relid = 1 ;relid <= k ; relid++)
		}//even sequence 
		
		else //odd path
		{
			for(int relid = 1 ;relid <= k ; relid++)	
			{				
				if(relid%2 == 1)
				{
					x = hkey2;
					y = hkey1;	
					oddrelationnum++;					
				}				
				if(relid%2 == 0)
				{
					x = hkey1;
					y = hkey2;
				}
				
					size = m+1;
					n =(int)Math.ceil((double)(k)/2);
					
					int[] coordinate = new int[n];
					int p=1;
					for(int l=n-1;l>=0&&p>0;p++)
					{
						
						if((l == n - 1) && (coordinate[oddrelationnum-2] == (Integer.parseInt(x.toString()) % m))) {
						int rednum= 0;
					
						for(int axis = 0 ; axis < n ; axis++)
						{
							rednum += (coordinate[axis] * Math.pow(m,axis));
						}
						String redid = Integer.toString(rednum);
						context.write(new LongWritable(Long.parseLong(x.toString())), new Text(relid +" " + redid + " "+ y));
						}
						coordinate[l]++;
					
						if(coordinate[l] == size - 1)
						 {
							coordinate[l] = 0;
							l--;
						 }
						 else if(l < n - 1)
						 {
						   while(l<n-1)
							l++;
						 }
					}//for(int l=n-1;l>=0&&p>=0;p++)
				}//for(int relid = 1 ;relid <= k ; relid++)
		}//odd sequence 
		
				
    }//map
 }//Class MAP
 
 
 
  public static class MyPartitioner extends Partitioner<LongWritable , Text>// find the reducer id from mapper output
  {
 
	@Override
	public int getPartition(LongWritable key , Text value , int num_of_reducer)
	{
		String [] valholdrid = value.toString().split(" ");
		int reducernum = Integer.parseInt(valholdrid[1]);
		return reducernum;	
	}	 
  } 
 

	
	 public static class Reduce  extends Reducer<LongWritable, Text, LongWritable,Text>
     {
		public static class edge // edge class having two vertices
		{
			int v1 , v2;	
			public edge(int v1,int v2)
			{
				this.v1 = v1;
				this.v2 = v2;	
			}
			public edge()
			{
				this.v1=0;
				this.v2=0;
			}
		}
	public static class RelationList // list of buckets
	{
		ArrayList<edge> list;
		public	RelationList()
		{
			list = new ArrayList<edge>();
		}

	} 

		static edge kpath[];
		static edge v1v2 = new edge();
		static RelationList relationbucket[] ;	
		int k,cycle;
		
	protected void setup(Context context) throws IOException,InterruptedException// variable setup class
	{
		Configuration conf = context.getConfiguration();
		k = Integer.parseInt(conf.get("k"));
		cycle=Integer.parseInt(conf.get("cycle"));
		relationbucket = new RelationList[k];
		kpath = new edge[k];
		for(int i = 0 ; i< k ; i++)
			kpath[i] = new edge();
		for(int i = 0 ; i< k ; i++)
			relationbucket[i] = new RelationList();
	}
	// find path using recursive method
		static void findKPath(edge p , RelationList relationlist[],int relseq, int maxrel,Context context) throws IOException,InterruptedException
		{
			for(int i = 0;i < relationlist[relseq].list.size() ; i++)
			{
				
				if(relationlist[relseq].list.get(i).v1 == p.v2)
				{
					kpath[relseq] = relationlist[relseq].list.get(i);
					if(maxrel > relseq)
					{
						findKPath(relationlist[relseq].list.get(i) , relationlist ,relseq+1, maxrel,context);
					}
					else
					{
						for(int j = 0 ; j < kpath.length ; j++)
						context.write(new LongWritable(kpath[j].v1) , new Text(" "+kpath[j].v2 + "\n" ));
					}
				}	
			}	
		} 
// find cycle using recursive method
		static void findKPathCycle(edge p , RelationList relationlist[],int relseq, int maxrel,Context context) throws IOException,InterruptedException
		{
			for(int i = 0;i < relationlist[relseq].list.size() ; i++)
			{
				if(relationlist[relseq].list.get(i).v1 == p.v2)
				{
					
					kpath[relseq] = relationlist[relseq].list.get(i);
					if(maxrel > relseq)
					{
						findKPathCycle(relationlist[relseq].list.get(i) ,relationlist,relseq+1,maxrel,context);
					}
					else
					{
							
						if(kpath[kpath.length-1].v2 == kpath[0].v1)
						for(int j = 0 ; j < kpath.length ; j++)
						context.write(new LongWritable(kpath[j].v1) , new Text(" "+kpath[j].v2 + "\n" ));
						
					}
				}	
			}	
		}		
	
         public void reduce(LongWritable key, Iterable<Text> values, Context context) throws  IOException ,InterruptedException 
				{ 
					
					String tempstr[];
					for (Text val : values) {
						tempstr = val.toString().split(" ");
						
						if(Integer.parseInt(tempstr[0])%2 == 0 )
						{
						edge p = new edge((int)(key.get()) , Integer.parseInt(tempstr[2]) );	
						relationbucket[Integer.parseInt(tempstr[0]) - 1 ].list.add(p);
						}
						
						else
						{
						edge p = new edge(Integer.parseInt(tempstr[2]),(int)(key.get()));	
						relationbucket[Integer.parseInt(tempstr[0]) - 1].list.add(p);	
						}		
					}                      
				}
				
		protected void cleanup(Context context) throws IOException,InterruptedException		
		{
		if(k!=1 && cycle==0)
		{	
			for(int i= 0 ; i < relationbucket[0].list.size() ; i++)
			{
				v1v2.v1 = 	relationbucket[0].list.get(i).v1;
				v1v2.v2 = 	relationbucket[0].list.get(i).v2;
				kpath[0] = v1v2;
				findKPath(v1v2 ,relationbucket,1 , k-1,context);	
			}	
		}
		else if(k==1&& cycle==0)
		{	
			for(int i= 0 ; i < relationbucket[0].list.size() ; i++)
			{
				v1v2.v1 = 	relationbucket[0].list.get(i).v1;
				v1v2.v2 = 	relationbucket[0].list.get(i).v2;
				context.write(new LongWritable(v1v2.v1) , new Text(" "+v1v2.v2+ "\n" ));
					
			}
			
		}

		else if(k!=1 && cycle==1)
		{	
			for(int i= 0 ; i < relationbucket[0].list.size() ; i++)
			{
				v1v2.v1 = 	relationbucket[0].list.get(i).v1;
				v1v2.v2 = 	relationbucket[0].list.get(i).v2;
				kpath[0] = v1v2;
				findKPathCycle(v1v2 ,relationbucket,1 , k-1,context);	
			}	
		}
		else if(k==1&& cycle==1)
		{	
			for(int i= 0 ; i < relationbucket[0].list.size() ; i++)
			{
				v1v2.v1 = 	relationbucket[0].list.get(i).v1;
				v1v2.v2 = 	relationbucket[0].list.get(i).v2;
				if(v1v2.v1==v1v2.v2)
				context.write(new LongWritable(v1v2.v1) , new Text(" "+v1v2.v2+ "\n" ));
				
			}
			
		}
		else
		{/*Do nothing*/ }

				
		}
		// find path using stack
		    /*   protected void cleanup(Context context)throws IOException,InterruptedException
 
			{ 
			Stack st=new Stack();
			String path="";
			for(int i= 0 ; i < relationbucket[0].list.size() ; i++)
			{
			
				RelationList r1=new RelationList();
				r1.add(relationbucket[0].list.get(i));
				st.add(r1)
			}
			while(!st.IsEmpty() )
			{
					int flag=0;
					RelationList r2=(RelationList)st.pop();
					for (int j=1;j<relationbucket.size();j++)
					   {
						   int len=r2.size();
							Record r3=(Record)r2.get(len-1);
							int flag=0;
							for(int t=0;t<rlist[k].size();t++)
							{
								Record r4=rlist[k].list.get[t];
								if(r3.v2==r4.v1)
								{
									RelationList r5=new RelationList(r2);
									r5.add(r4);
									st.add(r5);
								}
							}
							if(flag==0)
							{
								st.pop();
								break;
							}
					}
					if(pathlength==k)
					{
						context.write(new Text("Path"),new Text (path));
					}
				}*/
	}	
        
 public static void main(String[] args) throws Exception {
	 
	int m = 3; 
	double k = Double.parseDouble(args[2]);
	int mpowkby2 =(int)( Math.pow(m,Math.ceil(k/2)));
    if(args.length != 4)
    {
			System.out.println("\nUsage : Javafile  <inputdir> <outputdir> <k> <put 0 for path, 1 for cycle>");
			System.exit(0);
	}
    
    Configuration conf = new Configuration();
    conf.set("k", args[2]);
    conf.set("cycle",args[3]);	
    conf.set("m", Integer.toString(m));	    
    Job job = new Job(conf);
    job.setJarByClass(kLengthPath.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(mpowkby2);  
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));          
	job.waitForCompletion(true); 	
	}
}//Class kLengthPath.java 

