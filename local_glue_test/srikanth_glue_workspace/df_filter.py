

# s3://srikanth-tf-bucket-2027/data.json

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import lit,concat,substring,col,filter,sum,current_date,to_date
from pyspark.sql.types import IntegerType
import time

# Step 1: Set Delta configs BEFORE SparkSession is created
spark = SparkSession.builder \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .config("spark.driver.host", "0.0.0.0")\
    .config("spark.driver.bindAddress", "0.0.0.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Initialize Spark and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)


data = [
    (1, "sai", "sai2gmail",12000,"1997-01-25"),
    (2, "ramu", "ramui2gmail",15000,"1998-03-25"),
    (3, "sairamsai", "sairam2gmail",3000,"1996-07-25"),
    (4, "chandu", "chansai2gmail",80000,"2000-01-25"),
    (5, "bhai", "bhai@gmail",45000,"2021-08-25"),
    (6, "last", "lastsai@ymail",20000,"2025-01-25"),
    (7, "sangee", " ",350000,"2005-01-25")
]

fields = ["student_id", "student_name", "student_email","salary","enroll_date"]
df = spark.createDataFrame(data,schema = fields)
df.printSchema()
df = df.withColumn("student_id",col("student_id").cast(IntegerType()))
df = df.withColumn("salary",col("salary").cast(IntegerType()))
df = df.withColumn("enroll_date",to_date(col("enroll_date"),"yyyy-MM-dd"))

#df = df.withColumn("enroll_date",lit(current_date()))
df.show()
df.printSchema()
print("-----------------------------------------------------------------------------________________________________________________-----------")
dff1 = df.filter(col("salary") > 25000)
dff2 = df.filter((col("salary") > 25000) & (col("student_id")== 1))
dff3 = df.filter((col("salary") > 25000) | (col("student_id")== 1))
dff4 = df.filter(col("salary").between(25000,100000))
dff5 = df.filter(col("salary").isin(12000,15000,3000))
dff6 = df.filter(col("student_email").isNotNull())
dff7 = df.filter(col("student_email").isNull())
dff8 = df.filter(col("student_email").contains("bhai"))
dff1.show()
dff2.show()
dff3.show()
dff4.show()
dff5.show()
dff6.show()
dff7.show()
dff8.show()


print("_____________________________________________________________________________________________")
df.filter(col("salary") == 80000).show()
df.filter((col("salary") <= 10000) & (col("salary") >= 100000)).show()
df.filter((col("salary")<10000)|(col("salary")>100000)).show()
df.filter(col("salary").between(10000,100000)).show()
df.filter(col("student_email").isNotNull()).show()
df.filter(col("student_email").isNull()).show()
df.filter(col("salary").isin(80000,45000)).show()
df.filter(col("student_email").contains("ramu")).show()
df.filter(col("enroll_date").between("2000-01-01","2025-07-20")).show()

df.filter(col("salary") > 5000).orderBy(col("enroll_date").asc()).show()

df.printSchema()


df.filter(col("student_name").startswith("s")).show()
df.filter(col("student_email").endswith("@ymail")).show()
df.filter(col("student_email").rlike("lastsai")).show()
df.filter(col("student_email").contains("ymail")).show()

df.filter(col("salary")>35000).orderBy(col("salary").asc()).show()
df.filter(col("salary")> 35000).orderBy(col("enroll_date").asc()).show()

time.sleep(10)








