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



df1 = spark.read.load("s3://Div41/dest/customer_api")

df2 = spark.read.load("s3://Div41/dest/total_amount_data")

df3 = spark.read.load("s3://Div41/dest/site_count")

rm = df1.withColumn("username",regexp_replace(col("username"),"([0-9])",""))

from pyspark.sql.functions import *

finaljoin = rm.join(df2,["username"],"left").join(df3,["username"],"right")

finaljoin.write.mode("overwrite").save("s3://Div41/dest/finalcustomer")

finaljoin.limit(10).write.format("org.elasticsearch.spark.sql").option("es.nodes","https://search-es41-wysrpzmypr7huqty7ltfxibeca.aos.ap-us-east-2.on.aws").option("es.port","443").option("es.net.http.auth.user","root").option("es.net.http.auth.pass","Divya@usa908").option("es.nodes.wan.only","true").mode("overwrite").save("prod/resume")





# yarn application -list | awk '/application_/{print $1}' | xargs -I {} yarn application -kill {}


# pyspark --packages  org.elasticsearch:elasticsearch-spark-20_2.12:7.12.0,commons-httpclient:commons-httpclient:3.1



