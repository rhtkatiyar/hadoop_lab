import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
 
public class GraphGenerate
 {
	public static void main(String[] args)
	 {
		try
		 {
			
			int nodes = 5;
			double probability = 0.5d;
			
			File currentDir = new File(".");
			File file = new File(currentDir.getCanonicalPath()+"/graphInput.txt");
			if (file.exists())
			{
				file.delete();
			}
			
			file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for(int i=1; i<=nodes; i++)
			{
				for(int j=i+1; j<=nodes; j++)
				{
					if (Math.random() >= probability)
					{
						bw.write(""+i+"\t"+""+j+"\n");
					}
				}
			}
			bw.close();
 
			System.out.println("Done");

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

	

	}
}
