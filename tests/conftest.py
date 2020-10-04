import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("weather-data-loader") \
        .master("local[4]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    return spark