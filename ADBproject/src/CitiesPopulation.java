/*
 * 1. Computing Selection by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CitiesPopulation.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * 
 */

/* header files*/

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


public class CitiesPopulation extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//Checking the number of parameters entered
		if(args.length<2)
		{
			System.out.println("Please enter input and output directories correctly");
			return -1;
		}
		JobConf conf= new JobConf(CitiesPopulation.class);
		//Fetching the input and output directories from user
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(CitiesPopulationMapper.class);
		conf.setReducerClass(CitiesPopulationReducer.class);
		
		//Text for city name and intwritable for population
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		
		int exitCode=ToolRunner.run(new CitiesPopulation(),args);
		System.exit(exitCode);
	
	}

}
