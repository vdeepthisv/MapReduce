/*
 * 3. Computing Natural Join by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CountryLanguage.java
 * Table structure: 
 * Country(Code,Name,Continent,Region,SurfaceArea,IndepYear,Population,
 * LifeExpectancy,GNP,GNPold,LocalName,GovernmentForm,HeadOfState,Capital,Code2)
 * CountryLanguage(CountryCode,Language,IsOfficial,Percentage)
 * Input file: Country.txt,CountryLanguage.txt
 * 
 */
/*header files*/
import java.net.URI;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.ToolRunner;



public class CountryLanguage extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		 JobConf conf = new JobConf (CountryLanguage.class);
		//Checking the number of parameters entered
				if(args.length<2)
				{
					System.out.println("Please enter input and output directories correctly");
					return -1;
				}
				
      // Fetching country.txt which is stored under files directory in hadoop
	//file system
       try{
    	   DistributedCache.addCacheFile(new URI("files/country.txt"), conf);
      }
     catch(Exception ex)
      {
    	   System.out.println(ex);
      }
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setNumReduceTasks(0);
		
		//Fetching the input and output directories from user
		conf.setMapperClass(CountryLanguageMapper.class);
		conf.setReducerClass(CountryLanguageReducer.class);
		
				
		//country name and language are text
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		JobClient.runJob(conf);
		return 0;
	}
	//main function
public static void main(String[] args) throws Exception {
		
		int exitCode=ToolRunner.run(new CountryLanguage(),args);
		System.exit(exitCode);
	
	}


}
