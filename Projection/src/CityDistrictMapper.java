/*
 * 2. Computing Projection by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CityDistrictMapper.java
 * Table structure: City(ID,Name,CountryCode,District,population)
 * Input file: City.txt
 * Description:This program parses the file city.txt and select all the city
 * names along with their corresponding district 
 */

/*header files*/
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public  class CityDistrictMapper
	extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input_line = value.toString();
			String [] list = input_line.split(",");
			
				
				if (list.length == 5) {
				// Fetching the Name attribute from City
				String city = list[1];
				
				//Fetching the District attribute from City
				String district = list[3];

				//Displaying city-district names
				context.write(new Text(city), new Text(district));

			} else {
				System.out.println("The file city.txt could not be read correctly.");
			}
		}
	}