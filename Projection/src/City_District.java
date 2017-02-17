/*
 * 2. Computing Projection by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: City_District.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * Description:This program parses the file city.txt and select all the city
 * names along with their corresponding district 
 */

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class City_District extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		//Checking the number of parameters entered
		if(args.length<2)
		{
			System.out.println("Please enter input and output directories correctly");
			return -1;
		}
		
		Job conf = new Job(getConf());
		conf.setJarByClass(City_District.class);
		conf.setJobName("City_District");
		
		
		// Setting input and output path of input provided by user
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		//Setting the output format-  cityname and district are both in text format
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//Setting the input and output classes for mapper and reducer
		
		conf.setMapperClass(CityDistrictMapper.class);
		conf.setReducerClass(CityDistrictReducer.class);
		
		// city and district name are type text
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		boolean success = conf.waitForCompletion(true);

		return success ? 0 : 1;
	}
	/* program main function for execution */
public static void main(String[] args) throws Exception {
		
		int exitCode=ToolRunner.run(new City_District(),args);
		System.exit(exitCode);
	
	}

}
