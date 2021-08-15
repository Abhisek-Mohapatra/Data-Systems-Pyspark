# Last Modified :  April 3 , 11:56 pm 

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

conf = SparkConf().setAppName("Number of Airports by Country").setMaster("local")
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Number of Airports by Country") \
    .config("spark.some.config.option", "randomValue") \
    .getOrCreate()

airport_df = spark.read.csv("airports.csv", header=True,inferSchema=True)
new_df=airport_df.repartition(n_cpu)
result_df=new_df.groupBy("COUNTRY").count()
result_df = result_df.withColumnRenamed("count", "Count of Airports")

result_df.toPandas().to_csv(outFile,index=False)
