# Last Modified :  April 4, 8:05 pm

import pyspark
import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import *


n_cpu=int(sys.argv[1])
outFile=sys.argv[2]

if len(sys.argv)!=3:
    sys.exit("Incorrect number of arguments are provided")
elif n_cpu<=0:
    sys.exit("Incorrect number of cpu's are provided")

conf = SparkConf().setAppName("Country with Max Airports").setMaster("local")
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Country with Max Airports") \
    .config("spark.some.config.option", "randomValue") \
    .getOrCreate()

airport_df = spark.read.csv("airports.csv", header=True,inferSchema=True)
new_df=airport_df.repartition(n_cpu)
result_df=new_df.groupBy("COUNTRY").count()
result_df.createOrReplaceTempView("airportTable")
result_df=spark.sql("select COUNTRY from airportTable where count=(select max(count) from airportTable)")
result_df = result_df.withColumnRenamed("COUNTRY", "Country having maximum airports")

result_df.toPandas().to_csv(outFile,index=False)

