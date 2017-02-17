/*
 * 1. Computing Selection by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CitiesPopulationMapper.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * 
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class CitiesPopulationMapper extends MapReduceBase
implements Mapper <LongWritable,Text,Text,IntWritable>{
	

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter r)
			throws IOException {
		
		String input_line = value.toString();
		String [] list = input_line.split(",");
		
		if (list.length==5){
			// Fetching the Name attribute from City
			Text city=new Text (list[1]);
			
			//Fetching the population attribute from City
			Integer population= new Integer(list[4]);
			
			//Fetching only those cities with population > 300,000
			if (population > 300000)
			output.collect(new Text(city), new IntWritable(population));
			
		}
		else 
		{
			System.out.println("The file city.txt could not be read correctly.");
		}
		
			
	}
	
}
