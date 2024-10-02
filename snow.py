from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import os

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()


snowdf = (spark.read.format("snowflake")
          .option("sfURL","https://mwragln-xe38082.snowflakecomputing.com")
          .option("sfAccount","mwragln")
          .option("sfUser","divavdgusaia")
          .option("sfPassword","Divya0908")
          .option("sfDatabase","Divdb")
          .option("sfSchema","Divschema")
          .option("sfRole","ACCOUNTADMIN")
          .option("sfWarehouse","COMPUTE_WH")
          .option("dbtable","srctab")
          .load()
          )

snowdf.show()


from pyspark.sql.functions import *


aggdf = snowdf.groupBy("username").agg(count("site").alias("cnt"))

aggdf.show()

aggdf.write.mode("overwrite").save("s3://Div41/dest/site_count")