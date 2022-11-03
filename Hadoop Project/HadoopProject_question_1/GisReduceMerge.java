import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public  class GisReduceMerge extends Reducer<LongWritable, Text, Text, Text>
{       
      int []cities=new int[100];

public void setup(Context context) throws IOException, InterruptedException
      {
		for(int i=0;i<=99;i++)
		{
			cities[i]=0;
			//cities[1][i]=i;
		}
			
      }
class CityVisit implements Comparator<CityVisit>, Comparable<CityVisit>{
   private String name;
   private int value;
   CityVisit(){
   }

   CityVisit(String n, int a){
      name = n;
      value = a;
   }

   public String getCityVisitName(){
      return name;
   }

   public int getCityVisitValue(){
      return value;
   }

   // Overriding the compareTo method
   public int compareTo(CityVisit d){
      return (this.name).compareTo(d.name);
   }

   // Overriding the compare method to sort the age 
   public int compare(CityVisit d, CityVisit d1){
      return d.value - d1.value;
   }
}
List<CityVisit> list = new ArrayList<CityVisit>();
     public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
     {
		        for(Text t:values)
				{
					String []record=values.toString().split("\t");
					String []recordcities=record[15].split(":");
					for(int i=0;i<=99;i++)
					{
						cities[i]=cities[i]+Integer.parseInt(recordcities[i]);
					}
				}
     }
		public void cleanup(Context context)	throws IOException ,InterruptedException 
		{
			for (int i=0;i<=99;i++)
			{
				list.add(new CityVisit("C"+i, cities[i]));
				//CitiesNames.add(new CitiesName("C"+i, cities[i]));
				//context.write(new Text(rec.key+""), new Text(rec.sum+""));
			}
			Collections.sort(list, new CityVisit());
			//System.out.println(" ");
			for(CityVisit a: list)//printing the sorted list of ages
			context.write(new Text(""+a.getCityVisitName()), new Text(""+a.getCityVisitValue()));
			//System.out.print(a.getDogName() +"  : "+a.getDogAge() + ", ");
			
		}
}
