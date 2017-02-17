/*
 * 4. Computing Aggregation by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: DistrictCityCountMapper.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * Description:This program parses city.txt and displays the number of cities each district contains
 */
/*Header Files */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class DistrictCityCountMapper  extends MapReduceBase
implements Mapper <LongWritable,Text,Text,IntWritable>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter r)
			throws IOException {

		String input_line = value.toString();
		String [] list = input_line.split(",");
		
		if (list.length==5){
			// Fetching the District attribute from City
			Text district=new Text (list[3]);
			output.collect(new Text(district), new IntWritable(1));
			
		}
		else 
		{
			System.out.println("The file city.txt could not be read correctly.");
		}
		
		
	}


}
