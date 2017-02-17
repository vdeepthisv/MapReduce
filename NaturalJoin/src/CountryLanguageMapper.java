/*
 * 3. Computing Natural Join by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CountryLanguageMapper.java
 * Table structure: 
 * Country(Code,Name,Continent,Region,SurfaceArea,IndepYear,Population,
 * LifeExpectancy,GNP,GNPold,LocalName,GovernmentForm,HeadOfState,Capital,Code2)
 * CountryLanguage(CountryCode,Language,IsOfficial,Percentage)
 * Input file: Country.txt,CountryLanguage.txt
 * 
 */
/*header files*/
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.filecache.DistributedCache;


public class CountryLanguageMapper extends MapReduceBase
implements Mapper <LongWritable,Text,Text,Text> {
	
	String input;
	Map<String,String> country_details = new HashMap<String,String>();
	private Path[] local;
	
	@Override
 	public void configure(JobConf conf)
	{
		try {
			local = DistributedCache.getLocalCacheFiles(conf);
			for(Path p : local)
			{
					if(p.getName().contains("country.txt"))
					{
						BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
						
						//Reading input from country.txt
						String input = reader.readLine();
						while(input != null)
						{
							String[] tokens = input.split(",");
							//Fetching country code and name
							String code = tokens[0];
							String name = tokens[1];
							country_details.put(code, name);
							input = reader.readLine();
						}
						reader.close();
					}
		}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter report) throws IOException {
		
		input = value.toString();
		String[] tokens = input.split(",");
		
			//Fetch country code from countrylanguage.txt
			String countrycode = tokens[0];
			
			//Fetch language from countrylanguage.txt
			String language = tokens[1];
			
			//Fetch the bit- if language is official or not
			String isOfficial = tokens[2];
			
			if(language.contains("English") && isOfficial.contains("T"))
			{
				String display_country = null;
				display_country = country_details.get(countrycode);
				if(!display_country.isEmpty())
				output.collect(new Text(display_country), new Text(" "));					
			}
		
	}
	
}
	
	


