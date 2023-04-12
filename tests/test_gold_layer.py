import os
import pytest
from pyspark.sql import SparkSession
from modules.gold_layer import create_gold_layer
from pyspark_testing import assert_frame_equal

# Setup a local SparkSession
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Gold Layer Unit Test") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_create_gold_layer(spark):
    # Set environment variables for testing
    os.environ['S3_BUCKET'] = 'your_test_s3_bucket'

    # Call the create_gold_layer() function
    create_gold_layer()

    # Load the saved data from Redshift and verify the schema and row count
    gold_df = spark.read \
        .format("jdbc") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("url", "your_test_redshift_url") \
        .option("user", "your_test_redshift_user") \
        .option("password", "your_test_redshift_password") \
        .option("dbtable", "gold_layer") \
        .load()

    expected_schema = "week_start STRING, week_end STRING, product_id INT, total_quantity BIGINT"
    assert gold_df.schema.simpleString() == expected_schema
    assert gold_df.count() > 0
