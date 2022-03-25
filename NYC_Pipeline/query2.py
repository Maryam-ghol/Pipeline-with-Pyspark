###Day of the week in 2019 and 2020 which has the lowest number of single rider trips:

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
    print(rangeYears)
    TripsPerDaydfs=[]
    cabTypes=['yellow','green']
    for yearData in rangeYears:
        for monthYear in range(1,13):
            for cabType in cabTypes:
                fileName=cabType+'_tripdata_'+str(yearData)+'-'+str(monthYear)
                csvfile=FilePath+fileName+'.csv'
                if (cabType=='yellow' and exists(csvfile)):
                  Yellowdf=ExtractEntityFromParFile(spark,FilePath+fileName)
                  weekdayyellowdf=getWeekdaySingleriderYellowdf(spark,Yellowdf)
                  tripsPerDayofWeekY=getQuery2part2df(spark,weekdayyellowdf)
                  TripsPerDaydfs.append(tripsPerDayofWeekY)
                  LoadNYC(tripsPerDayofWeekY,'perdayYellow2_'+fileName) 
                elif (cabType=='green' and exists(csvfile)):
                  Greendf=ExtractEntityFromParFile(spark,FilePath+fileName)
                  weekdaygreendf=getWeekdaySingleriderGreendf(spark,Greendf)
                  tripsPerDayofWeekG=getQuery2part2df(spark,weekdaygreendf)
                  TripsPerDaydfs.append(tripsPerDayofWeekG)
                  LoadNYC(tripsPerDayofWeekG,'perdayGreen_'+fileName)
                    
    PerDayofAlldf= reduce(DataFrame.unionAll, TripsPerDaydfs)
    Resultquery2ofAlldf=getQuery2part3df(spark,PerDayofAlldf)
    LoadNYC(Resultquery2ofAlldf,'query2'+'result') 
   


def getQuery2part2df(spark, weekdaydf):
      print('query22222222222222222222222222222222222222222222222222222222222part2222222222222')
      weekdaydf.createOrReplaceTempView('NYC')
      query = '''
           SELECT distinct day,count(*) OVER(partition by day) as count_tripsperweekday   from NYC where year='2019' or year='2020' order by count_tripsperweekday  limit 1;
            '''
      return spark.sql(query) 


def getQuery2part3df(spark, Most3ofAlldf):
      print('query22222222222222222222222222222222222222222222222222222222222part33333333333333')
      Most3ofAlldf.createOrReplaceTempView('NYC')
      query = '''
           SELECT distinct day,sum(count_tripsperweekday) OVER(partition by day) as count_All_tripsperweekday   from NYC  order by count_All_tripsperweekday   ;
            '''
      return spark.sql(query) 


def WriteDataframeToCSV(df, outFile):
      print('loaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')
      print(FilePath+outFile)
      df.repartition(1).write.mode('overwrite').csv(FilePath+outFile)


def getWeekdaySingleriderGreendf(spark, dfNYC):
      print('weeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeekday')
      #dfNYChour=dfNYC.withColumn("Day", substring("lpep_pickup_datetime", 0, 20))
      dfNYCweek=dfNYC.withColumn('Day', f.date_format("lpep_pickup_datetime", 'EEEE'))
      dfNYweekNYear=dfNYCweek.withColumn('year', f.date_format("lpep_pickup_datetime", 'yyyy'))
      dfNYweekNYear.createOrReplaceTempView('NYC')
      query = '''
           SELECT  *   from NYC  where passenger_count=1 ;
            '''
      return spark.sql(query)  

def getWeekdaySingleriderYellowdf(spark, dfNYC):
      print('weeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeekday')
      #dfNYChour=dfNYC.withColumn("Day", substring("lpep_pickup_datetime", 0, 20))
      dfNYCYweek=dfNYC.withColumn('Day', f.date_format("tpep_pickup_datetime", 'EEEE'))
      dfNYweekNYearY=dfNYCYweek.withColumn('year', f.date_format("tpep_pickup_datetime", 'yyyy'))
      dfNYweekNYearY.createOrReplaceTempView('NYC')
      query = '''
           SELECT  *   from NYC  where passenger_count=1 ;
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
                