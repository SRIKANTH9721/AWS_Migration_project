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

# S3 path (CSV format)
# input_path = "s3://srikanth-tf-bucket-2027/bronze_data_storage/discounts.csv"

# # Read data into a DataFrame
# df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# df = df.withColumnRenamed("Sub Category", "SubCategory")

# df.show()

# output_path = "s3://srikanth-tf-bucket-2027/gold_data_storage/discounts/"
# df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_path)


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
# dff1 = df.filter(col("salary") > 25000)
# dff2 = df.filter((col("salary") > 25000) & (col("student_id")== 1))
# dff3 = df.filter((col("salary") > 25000) | (col("student_id")== 1))
# dff4 = df.filter(col("salary").between(25000,100000))
# dff5 = df.filter(col("salary").isin(12000,15000,3000))
# dff6 = df.filter(col("student_email").isNotNull())
# dff7 = df.filter(col("student_email").isNull())
# dff8 = df.filter(col("student_email").contains("bhai"))
# dff1.show()
# dff2.show()
# dff3.show()
# dff4.show()
# dff5.show()
# dff6.show()
# dff7.show()
# dff8.show()


print("_____________________________________________________________________________________________")
df.filter(col("salary") == 80000).show()
df.filter((col("salary") <= 10000) & (col("salary") >= 100000)).show()
df.filter((col("salary")<10000)|(col("salary")>100000)).show()
# df.filter(col("salary").between(10000,100000)).show()
# df.filter(col("student_email").isNotNull()).show()
# df.filter(col("student_email").isNull()).show()
# df.filter(col("salary").isin(80000,45000)).show()
# df.filter(col("student_email").contains("ramu")).show()
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

#df = df.groupBy(col("student_id")).agg(sum("salary").alias("total_salary"))

#print("\n✅ Spark UI should be running at http://localhost:4040\n")
#print("Sleeping for 1 minutes to keep Spark UI alive...")








"""df =  df.withColumn("class",lit(5))
df =  df.withColumn("Country",lit("INDIA"))
df = df.withColumn("concat_clm",concat(df["student_id"],df["student_name"]))
df = df.withColumn("substring_clm",substring(col("student_email"),1,3)) 
df.printSchema()
df.show() 
print("\n✅ Spark UI should be running at http://localhost:4040\n")
print("Sleeping for 1 minutes to keep Spark UI alive...")
time.sleep(10)
df_ft1 = df.filter((col("student_id")==1) | (col("student_name")== "bhai"))
df_ft1.show()
df_ft2 = df.filter((col("student_id")==1) & (col("student_id")==2))
df_ft2.show()

df3 = df.filter(col("student_id").isin(1,2,3,4))
df3.show()

df4 = df.filter(~col("student_id").isin(1,2,3,4))
df4.show() """





"""records = [
  {
    "student_id": 1,
    "student_name": "sai",
    "student_email": "sai2gmail"
  },
  {
    "student_id": 2,
    "student_name": "ramu",
    "student_email": "ramui2gmail"
  },
  {
    "student_id": 3,
    "student_name": "sairamsai",
    "student_email": "sairam2gmail"
  },
  {
    "student_id": 4,
    "student_name": "chandu",
    "student_email": "chansai2gmail"
  },
  {
    "student_id": 5,
    "student_name": "bhai",
    "student_email": "bhai@gmail"
  },
  {
    "student_id": 6,
    "student_name": "srikanthchindam",
    "student_email": "srikanthchindam@gmail"
  }
]

df = spark.createDataFrame(records)
df.printSchema()
df.show()"""