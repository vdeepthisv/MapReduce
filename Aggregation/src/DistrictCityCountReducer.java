/*
 * 4. Computing Aggregation by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: DistrictCityCountReducer.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * Description:This program parses city.txt and displays the number of cities each district contains
 */
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class DistrictCityCountReducer  extends MapReduceBase
implements Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter r)
			throws IOException {
		
		int city_count = 0;
		while (values.hasNext())
		{
			
			IntWritable itr = values.next();
			city_count += itr.get();
		
		}
		output.collect(key, new IntWritable(city_count));
	}

}
