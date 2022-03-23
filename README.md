# New York City Taxi and For-Hire Vehicle Data
Getting data from below url
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Instructions

##### 1. Install python and pyspark 

spark and jdk are prerequisites of pyspark. Compatibility of them is important based on their versions.  

##### 2. Download raw data

./downloadCSVFiles.py

Dowloads data of last three years. If the files are available in ./NYCfiles dowloading will be ignored for that file

##### 3. create Avro file

./AvroCreation.py
Based on NYCgreen.avsc and NYCyellow.avsc(schemas) avro files will be created. 

##### 4. Create Parquest file
./ParquetCreation.py
extract transforms and loads data in parquet files. one for yellow taxies and one for green

