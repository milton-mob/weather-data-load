from pyspark.sql import SparkSession
from app.config import get_spark_context
from app.app import run

def main():
    """Application entrypoint"""
    spark = get_spark_context()
    run(spark)


if __name__ == "__main__":
    main()
