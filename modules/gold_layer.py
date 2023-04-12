import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.settings import S3_CONFIG, REDSHIFT_CONFIG
from utils.logger import logger

def create_gold_layer():
    spark = SparkSession.builder \
        .appName("Gold Layer") \
        .getOrCreate()

    # Read Silver data from S3
    silver_df = spark.read \
        .parquet(f"s3a://{S3_CONFIG['bucket']}/silver_layer/")

    # Apply aggregations
    gold_df = silver_df \
        .groupBy("week_start", "week_end", "product_id") \
        .agg(sum("quantity").alias("total_quantity"))

    # Save the Gold dataframe to Redshift
    gold_df.write \
        .format("jdbc") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("url", REDSHIFT_CONFIG['url']) \
        .option("user", REDSHIFT_CONFIG['user']) \
        .option("password", REDSHIFT_CONFIG['password']) \
        .option("dbtable", "gold_layer") \
     	  .option("batchsize", 10000) \
        .mode("overwrite") \
        .save()
