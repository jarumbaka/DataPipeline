import pyspark
from pyspark.sql import SparkSession
from config.settings import DB_CONFIG, KAFKA_CONFIG, S3_CONFIG
from utils.logger import logger

def create_raw_layer():
    spark = SparkSession.builder \
        .appName("Raw Layer") \
        .getOrCreate()

    # Read data from MySQL
    sales_df = spark.read \
        .format("jdbc") \
        .option("driver", DB_CONFIG['driver']) \
        .option("url", DB_CONFIG['url']) \
        .option("user", DB_CONFIG['user']) \
        .option("password", DB_CONFIG['password']) \
        .option("dbtable", "sales_transactions") \
        .load()

    # Write data to Kafka
    sales_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("topic", KAFKA_CONFIG['topic']) \
        .save()

    # Read data from Kafka
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", KAFKA_CONFIG['topic']) \
        .option("startingOffsets", "earliest") \
        .load()

    # Save the Kafka dataframe to S3 as Parquet
    kafka_df.write \
        .parquet(f"s3a://{S3_CONFIG['bucket']}/raw_layer/")
