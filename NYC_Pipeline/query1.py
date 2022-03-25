##The average distance driven by yellow and green taxis per hour:

from asyncio.windows_events import NULL
from fileinput import filename
import pyspark
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f
from datetime import date
from os.path import exists
from functools import reduce
from pyspark.sql import DataFrame
import os
import glob
from pyspark.sql.functions import date_trunc,date_format, col,substring

FilePath = '.\\NYCfiles\\'

def ExtractEntityFromParFile(spark, parfilepath):
      print('extraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaact')
      print(parfilepath)
      os.chdir(parfilepath)
      resultparfile = glob.glob('*.{}'.format('parquet'))
      print('parquetfile')
      parfile=parfilepath+'\\'+resultparfile[0]
      df = spark.read.parquet(parfile) 
      os.chdir('..\\..')
      return df

def myNYC(spark) :
    rangeYears= range(date.today().year-3,date.today().year)
    cabTypes=['yellow','green']
    AllHoursDistancedfs=[]
    for yearData in rangeYears:
        for monthYear in range(1,13):
            for cabType in cabTypes:
                fileName=cabType+'_tripdata_'+str(yearData)+'-'+str(monthYear)
                csvfile=FilePath+fileName+'.csv'
                if (cabType=='yellow' and exists(csvfile)):
                  Yellowdf=ExtractEntityFromParFile(spark,FilePath+fileName)
                  hourlyTripsYellowdf=getHourlyYellowdf(spark,Yellowdf)
                  AllHoursDistancedfs.append(hourlyTripsYellowdf)
                  LoadNYC(hourlyTripsYellowdf,'query_1_hourly_'+fileName) 
                elif (cabType=='green' and exists(csvfile)):
                  Greendf=ExtractEntityFromParFile(spark,FilePath+fileName)
                  hourlyTripsGreendf=getHourlyGreendf(spark,Greendf)
                  AllHoursDistancedfs.append(hourlyTripsGreendf)
                  LoadNYC(hourlyTripsGreendf,'query_1_hourly_'+fileName) 

    AllHoursDistancedf= reduce(DataFrame.unionAll, AllHoursDistancedfs)                   
    AllDistanceNNumPerHourdf=getQuery1part1df(spark,AllHoursDistancedf)
    Resultquery1df=getQuery1_part2df(spark,AllDistanceNNumPerHourdf)
    LoadNYC(Resultquery1df,'query_1_result') 


def getQuery1part1df(spark, hourlyGreendf):
      print('query11111111111111111111111111111111111111111111111111111111111111part11111111111')
      hourlyGreendf.createOrReplaceTempView('NYC')
      query = '''
           SELECT distinct  hour,sum(distance_per_hour)  OVER(partition BY hour) as All_distance_per_Hour , sum(number_of_trips)  OVER(partition BY hour) as All_trips_per_Hour from NYC  order by hour   ;
            '''
      return spark.sql(query) 

def getQuery1_part2df(spark, hourlyGreendf):
      print('query11111111111111111111111111111111111111111111111111111111111111part222222222222')
      hourlyGreendf.createOrReplaceTempView('NYC')
      query = '''
           SELECT hour, All_distance_per_Hour/All_trips_per_Hour  from NYC  order by hour   ;
            '''
      return spark.sql(query)

def WriteDataframeToCSV(df, outFile):
      print('loaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')
      print(FilePath+outFile)
      df.repartition(1).write.mode('overwrite').csv(FilePath+outFile)



def getHourlyYellowdf(spark, dfNYC):
      print('houuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuurly')
      dfNYChour=dfNYC.withColumn('Hour', f.date_format("tpep_pickup_datetime", 'HH'))
      dfNYChour.createOrReplaceTempView('NYC')
      query = '''
          SELECT distinct hour,sum(trip_distance) Over(partition by hour)  as distance_per_hour, count(*) Over(partition by hour) as number_of_trips   from NYC    ;
            '''
      return spark.sql(query)  

def getHourlyGreendf(spark, dfNYC):
      print('houuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuurly')
      dfNYChour=dfNYC.withColumn('Hour', f.date_format("lpep_pickup_datetime", 'HH'))
      dfNYChour.createOrReplaceTempView('NYC')
      query = '''
           SELECT distinct hour,sum(trip_distance) Over(partition by hour)  as distance_per_hour, count(*) Over(partition by hour) as number_of_trips   from NYC    ;
            '''
      return spark.sql(query) 

def LoadNYC(myNYC,outFile):
      WriteDataframeToCSV(myNYC, outFile)

if __name__ == "__main__":
      spark = SparkSession.builder \
       .master("local") \
       .appName("parquet_example") \
       .getOrCreate()
      myNYC (spark)
                
      spark.stop()
                