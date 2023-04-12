import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.settings import S3_CONFIG
from utils.logger import logger

def create_silver_layer():
    spark = SparkSession.builder \
        .appName("Silver Layer") \
        .getOrCreate()

    # Read raw data from S3
    raw_df = spark.read \
        .parquet(f"s3a://{S3_CONFIG['bucket']}/raw_layer/")

    # Apply transformations
    silver_df = raw_df \
        .withColumn("week_start", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("week_end", date_add(to_date(col("date"), "yyyy-MM-dd"), 6))

    # Save the Silver dataframe to S3 as Parquet
    silver_df.write \
        .parquet(f"s3a://{S3_CONFIG['bucket']}/silver_layer/")
