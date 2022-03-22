from asyncio.windows_events import NULL
import pyspark
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f
from datetime import date
from os.path import exists
from functools import reduce
from pyspark.sql import DataFrame

FilePath = '.\\NYCfiles\\'

def ExtractEntityFromCSVFile(spark, filename):
      print('extraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaact')
      print(filename)
      df = spark.read.csv(filename, header = True)
      return df

def DimNYC(spark) :
      rangeYears= range(date.today().year-3,date.today().year)
      print(rangeYears)
      cabTypes=['yellow','green']
      Yellowdfs=[]
      Greendfs=[]
      for yearData in rangeYears:
        for monthYear in range(1,13):
            for cabType in cabTypes:
                fileName=cabType+'_tripdata_'+str(yearData)+'-'+str(monthYear)
                csvfile=FilePath+fileName+'.csv'
                if (cabType=='yellow' and exists(csvfile)):
                  Yellowdfs.append(ExtractEntityFromCSVFile(spark,csvfile))
                elif (cabType=='green' and exists(csvfile)):
                  Greendfs.append(ExtractEntityFromCSVFile(spark,csvfile))


      YellowAlldf= reduce(DataFrame.unionAll, Yellowdfs)
      GreenAlldf= reduce(DataFrame.unionAll, Greendfs)
      dimYellowNYC=TransformNYC(spark,YellowAlldf)
      dimGreenNYC=TransformNYC(spark,GreenAlldf)
      LoadNYC(dimYellowNYC,outFile='Yellow')
      LoadNYC(dimGreenNYC,outFile='Green')

               

def WriteDataframeToParquet(df, outFile):
      print('loaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad')
      df.repartition(1).write.mode('overwrite').parquet(FilePath+outFile)

def TransformNYC(spark, dfNYC):
      print('transfoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooorm')
      dfNYC.createOrReplaceTempView('NYC')
      query = '''
            select * from NYC order by 1
            '''
      return spark.sql(query)

def LoadNYC(dimNYC,outFile):
      WriteDataframeToParquet(dimNYC, outFile)



if __name__ == "__main__":
      spark = SparkSession.builder \
       .master("local") \
       .appName("parquet_example") \
       .getOrCreate()
      DimNYC (spark)
                
      spark.stop()
                