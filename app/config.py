from pyspark.sql import SparkSession

source = "./data"
fields = ["ObservationTime", "ObservationDate", "ScreenTemperature", "region"]
overall_fields = ["ObservationTime", "ObservationDate", "aggregated_date_temp", "region"]

def get_spark_context() -> SparkSession:
    spark = SparkSession.builder\
            .appName("weather-data-loader")\
            .master("local[4]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()

    return spark