/*
 * 1. Computing Selection by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CitiesPopulationReducer.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * 
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

// This method merges same cities and displays distinct cities with their population
public class CitiesPopulationReducer  extends MapReduceBase
implements Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
	OutputCollector<Text, IntWritable> output, Reporter r)
	throws IOException {
	
		
		while (values.hasNext())
		{
			int value = values.next().get();			
				output.collect(key, new IntWritable(value));
		
		}
	
	

}

}

