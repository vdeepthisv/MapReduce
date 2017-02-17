/*
 * 3. Computing Natural Join by mapreduce
 * Author: Vijaya Deepthi Srivoleti
 * File name: CountryLanguageReducer.java
 * Table structure: 
 * Country(Code,Name,Continent,Region,SurfaceArea,IndepYear,Population,
 * LifeExpectancy,GNP,GNPold,LocalName,GovernmentForm,HeadOfState,Capital,Code2)
 * CountryLanguage(CountryCode,Language,IsOfficial,Percentage)
 * Input file: Country.txt,CountryLanguage.txt
 * 
 */
/*header files*/
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public abstract class CountryLanguageReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text> {

	

	@Override
	public void reduce(Text key, Iterator<Text> value,
			OutputCollector<Text, Text> output, Reporter r) throws IOException {
		output.collect(new Text(key), new Text(" "));	
		
	}

}
