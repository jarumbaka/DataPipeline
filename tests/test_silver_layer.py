import os
import pytest
from pyspark.sql import SparkSession
from modules.silver_layer import create_silver_layer
from pyspark_testing import assert_frame_equal

# Setup a local SparkSession
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Silver Layer Unit Test") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_create_silver_layer(spark):
    # Set environment variables for testing
    os.environ['S3_BUCKET'] = 'your_test_s3_bucket'

    # Call the create_silver_layer() function
    create_silver_layer()

    # Load the saved data from S3 and verify the schema and row count
    silver_df = spark.read.parquet(f"s3a://{os.environ['S3_BUCKET']}/silver_layer/")
    expected_schema = "id INT, date STRING, store_id INT, product_id INT, quantity INT, week_start STRING, week_end STRING"
    assert silver_df.schema.simpleString() == expected_schema
    assert silver_df.count() > 0
