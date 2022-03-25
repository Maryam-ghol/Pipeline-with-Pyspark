##The top 3 of the busiest hours:

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
    AllHoursdfs=[]
    for yearData in rangeYears:
        for monthYear in range(1,13):
            for cabType in cabTypes:
                fileName=cabType+'_tripdata_'+str(yearData)+'-'+str(monthYear)
                csvfile=FilePath+fileName+'.csv'
                if (cabType=='yellow' and exists(csvfile)):
                  Yellowdf=ExtractEntityFromParFile(spark,FilePath+fileName)
                  hourlyTripsYellowdf=getHourlyYellowdf(spark,Yellowdf)
                  AllHoursdfs.append(hourlyTripsYellowdf)
                  LoadNYC(hourlyTripsYellowdf,'query_3_hourly_'+fileName) 
                  #LoadNYC(query1Yellowdf,'hour'+fileName)
                elif (cabType=='green' and exists(csvfile)):
                  Greendf=ExtractEntityFromParFile(spark,FilePath+fileName)
                  hourlyTripsGreendf=getHourlyGreendf(spark,Greendf)
                  AllHoursdfs.append(hourlyTripsGreendf)
                  LoadNYC(hourlyTripsGreendf,'query_3_hourly_'+fileName) 

    AllHoursdf= reduce(DataFrame.unionAll, AllHoursdfs)                   
    ResultMostBussiestdf=getQuery3df(spark,AllHoursdf)
    LoadNYC(ResultMostBussiestdf,'query_3_result') 


def WriteDataframeToCSV(df, outFile):
      print('loaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')
      print(FilePath+outFile)
      df.repartition(1).write.mode('overwrite').csv(FilePath+outFile)

def getQuery3df(spark, hourlyGreendf):
      print('query33333333333333333333333333333333333333333333333part1111111111111111111111111')
      hourlyGreendf.createOrReplaceTempView('NYC')
      query = '''
           SELECT distinct  hour,sum(trips_per_hour)  OVER(partition BY hour) as All_trips_per_Hour from NYC  order by All_trips_per_Hour desc  limit 3 ;
            '''
      return spark.sql(query) 


def getHourlyYellowdf(spark, dfNYC):
      print('houuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuurly')
      dfNYChour=dfNYC.withColumn('Hour', f.date_format("tpep_pickup_datetime", 'HH'))
      dfNYChour.createOrReplaceTempView('NYC')
      query = '''
          SELECT distinct hour,count(*) Over(partition by hour)  as trips_per_hour  from NYC    ;
            '''
      return spark.sql(query)  

def getHourlyGreendf(spark, dfNYC):
      print('houuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuurly')
      dfNYChour=dfNYC.withColumn('Hour', f.date_format("lpep_pickup_datetime", 'HH'))
      dfNYChour.createOrReplaceTempView('NYC')
      query = '''
           SELECT distinct hour,count(*) Over(partition by hour)  as trips_per_hour  from NYC    ;
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
                