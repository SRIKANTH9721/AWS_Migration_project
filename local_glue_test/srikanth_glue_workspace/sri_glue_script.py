# s3://srikanth-tf-bucket-2027/data.json

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

# Step 1: Set Delta configs BEFORE SparkSession is created
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Initialize Spark and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)

# S3 path (CSV format)
input_path = "s3://srikanth-tf-bucket-2027/bronze_data_storage/discounts.csv"

# Read data into a DataFrame
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

df = df.withColumnRenamed("Sub Category", "SubCategory")

df.show()

output_path = "s3://srikanth-tf-bucket-2027/gold_data_storage/discounts/"
df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_path)