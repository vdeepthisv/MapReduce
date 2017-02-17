/*
 * 4. Computing Aggregation by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: DistrictCityCount.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * Description:This program parses city.txt and displays the number of cities each district contains
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DistrictCityCount  extends Configured implements Tool{


	@Override
	public int run(String[] args) throws Exception {
		JobConf conf= new JobConf(DistrictCityCount.class);
		
		//Fetching the input and output directories from user
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		//Mapper and reducer classes
		conf.setMapperClass(DistrictCityCountMapper.class);
		conf.setReducerClass(DistrictCityCountReducer.class);
		
		//District-text and count- number
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
		return 0;
		
		
	}
	public static void main(String[] args) throws Exception {

		int exitCode=ToolRunner.run(new DistrictCityCount(),args);
		System.exit(exitCode);

	}



}
