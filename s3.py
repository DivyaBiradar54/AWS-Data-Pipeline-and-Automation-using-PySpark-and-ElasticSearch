from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import os

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import *


df = spark.read.format("csv").option("header","true").load("s3://Div41/src/2024-09-28/customer.csv")

df.show()

from pyspark.sql.functions import   *

aggdf = df.groupBy("username").agg(sum("amount").alias("total")).withColumn("total",expr("cast(total as int)"))

aggdf.show()


aggdf.write.mode("overwrite").save("s3://Div41/dest/total_amount_data")