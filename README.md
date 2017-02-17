•	Programming Environment: Hadoop and Java in Linux platform

•	Database Schema:

City (ID, Name, CountryCode, District, population)
Country (Code, Name, Continent, Region, SurfaceArea, IndepYear, Population,LifeExpectancy, GNP, GNPold, LocalName,          GovernmentForm, HeadOfState, Capital,Code2)
CountryLanguage (CountryCode, Language, IsOfficial, Percentage)


•	MapReduce
1.	Computing Selection by MapReduce: 

Find cities whose population is larger than 300,000.
This program parses the table city.txt and selects those cities with population greater than 300,000. From the input file city.txt, we first split each attribute and parse the file to get the city name and the population. We then select and display all the cities with population greater than 300,000.

Execution: Hadoop jar Selection.jar CitiesPopulation files/city.txt SelectionOutput
Output: Hadoop fs -cat SelectionOutput/part-00000

2.	Computing Projection by MapReduce:
Find all the name of the cities and corresponding district

For this section, the program parses the table city.txt and selects cities and their corresponding district. From the input file city.txt, we first split each attribute and parse the file to get the city name and the district names. The mapper function will output the city and district name. The input for the reducer function takes the output of the mapper function and connects all the strings that belong to the same city key.

Execution:Hadoop jar Projection.jar City_District files/city.txt ProjectionOutput
Output:	Hadoop fs -cat ProjectionOutput/part-r-00000

3.	Computing Natural Join by MapReduce
Find all countries whose official language is English

This program uses two files – country.txt and countrylanguage.txt. From country table, we fetch the country code and country name.
We join contry.txt and countrylanguage.txt with countrycode. From countrylanguage.txt, we check those records with language=’English’ and IsOfficial=’True’. We display the corresponding countries.

Execution: Hadoop jar NatualJoin.jar CountryLanguage files/countrylanguage.txt NaturalJoinOutput
Output: Hadoop fs -cat NaturalJoinOutput/part-00000


4.	Aggregation by MapReduce :
Find how many cities each district has.
This program parses the table city.txt and selects the cities and districts from the file. From the input file city.txt, we first split each attribute and parse the file to get the city name and the district. The mapper function will output the city and district name. The input for the reducer function counts the number of cities per district and displays the count for each city.
Execution: Hadoop jar Aggregation.jar DistrictCityCount files/city.txt AggregationOutput
Output: Hadoop fs -cat AggregationOutput /part-00000

