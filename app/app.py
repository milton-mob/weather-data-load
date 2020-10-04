from pyspark.sql import SparkSession, DataFrame
from cytoolz import pipe
import app.config as conf
from app.weather_processor import (
highest_temp,
hottest_date,
hottest_temp,
hottest_region,
hottest_overall_date
)


def run(spark: SparkSession):
    """Orchestrate execution"""
    df = load_data(spark)

    hottest_temp_rec = highest_temp(df)
    temp_d = hottest_date(hottest_temp_rec)
    temp_t= hottest_temp("ScreenTemperature", hottest_temp_rec)
    temp_r = hottest_region(hottest_temp_rec)

    overall_hottest = hottest_overall_date(df)
    dt_d = hottest_date(overall_hottest)
    dt_t= hottest_temp("aggregated_date_temp", overall_hottest)
    dt_r = hottest_region(overall_hottest)

    print("==================================================================================")
    print(f" date when the hottest temperature was collected: {temp_d[0][0:10]}, time when it reached the hottest temperature during the day: {temp_d[1]}")
    print(f" hottest temperature collected on the date: {temp_t[0]}")
    print(f" region where the hottest temperature was collected: {temp_r[0]}")

    print(f" date with overall hottest temperature: {dt_d[0][0:10]}")
    print(f" overall hottest temperature: {dt_t[0]}")
    print(f" region with the overall hottest temperature: {dt_r[0]}")
    print("==================================================================================")



def load_data(spark: SparkSession) -> DataFrame:
    """Load data from csv files"""
    path = conf.source
    return read_csv(spark, path)


def read_csv(
    spark: SparkSession,
    path: str,
    header: bool = True,
    infer_schema: bool = True,
    schema=None,
) -> DataFrame:
    """Load csv files from the source directory into a dataframe"""
    df = spark.read.option("inferSchema", str(infer_schema).lower())\
              .csv(path, header=header, schema=schema)
    return df


