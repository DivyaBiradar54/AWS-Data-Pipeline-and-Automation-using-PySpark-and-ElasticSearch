from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import os

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

import os
import urllib.request
import ssl

import os
import urllib.request
import ssl


urldata=(

    urllib
    .request
    .urlopen("https://randomuser.me/api/0.8/?results=500",context=ssl._create_unverified_context())
    .read()
    .decode('utf-8')

)

print(urldata)


rdd = sc.parallelize([urldata],1)

df = spark.read.json(rdd)

df.show()

df.printSchema()

from pyspark.sql.functions import *

flatten1= df.withColumn("results",expr("explode(results)"))

flatten1.show()

flatten1.printSchema()


flatten2= flatten1.selectExpr(

    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.gender",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.street",
    "results.user.location.zip",
    "results.user.md5",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.name.title",
    "results.user.password",
    "results.user.phone",
    "results.user.picture.large",
    "results.user.picture.medium",
    "results.user.picture.thumbnail",
    "results.user.registered",
    "results.user.salt",
    "results.user.sha1",
    "results.user.sha256",
    "results.user.username",
    "seed",
    "version"


)



flatten2.show()

flatten2.printSchema()



flatten2.write.mode("overwrite").save("s3://Div41/dest/customer_api")
