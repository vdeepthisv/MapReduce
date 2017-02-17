/*
 * 2. Computing Projection by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CityDistrictReducer.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * Description:This program parses the file city.txt and select all the city
 * names along with their corresponding district 
 */

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


	public  class CityDistrictReducer 
	extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
		
			String sum_districts = "";
			Iterator<Text> iter = values.iterator();
			
				while (iter.hasNext()) {
				String districtcount = iter.next().toString();
				sum_districts += districtcount;
			}
			context.write(new Text(key), new Text(sum_districts));
		}
	}