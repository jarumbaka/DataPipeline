import os
import pytest
from pyspark.sql import SparkSession
from modules.raw_layer import create_raw_layer
from pyspark_testing import assert_frame_equal

# Setup a local SparkSession
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Raw Layer Unit Test") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_create_raw_layer(spark):
    # Set environment variables for testing
    os.environ['DB_URL'] = 'your_test_db_url'
    os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'your_test_kafka_bootstrap_servers'
    os.environ['S3_BUCKET'] = 'your_test_s3_bucket'

    # Call the create_raw_layer() function
    create_raw_layer()

    # Load the saved data from S3 and verify the schema and row count
    raw_df = spark.read.parquet(f"s3a://{os.environ['S3_BUCKET']}/raw_layer/")
    expected_schema = "id INT, date STRING, store_id INT, product_id INT, quantity INT"
    assert raw_df.schema.simpleString() == expected_schema
    assert raw_df.count() > 0
