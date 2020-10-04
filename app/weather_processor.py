from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.functions import date_format
import app.config as conf


def highest_temp(df: DataFrame) -> DataFrame:
    """ Select the record with the highest temp"""
    df.repartition("ScreenTemperature")
    window = Window.orderBy(F.col("ScreenTemperature").desc())
    hot = (df.withColumn("row_num", F.row_number().over(window))
           .filter(F.col("row_num") == 1)
           .drop("row_num")
           .dropDuplicates()
           )
    return hot.select(*conf.fields)


def hottest_date(df: DataFrame) -> DataFrame:
    """Convert the date column to string format, then convert the dataframe to a list"""
    dt = df.withColumn("date_string", date_format(F.col("ObservationDate"), "yyyy-MM-dd hh:mm:ss")) \
        .select("date_string", "ObservationTime") \
        .drop("ObservationDate")

    return dt.toPandas()[dt.columns].values.tolist()[0]


def hottest_temp(col_name: str, df: DataFrame) -> list:
    """Get a single conlumn and convert the dataframe to list"""
    return df.select(col_name).toPandas()[col_name].values.tolist()


def hottest_region(df: DataFrame) -> list:
    """Get a single conlumn and convert the dataframe to list"""
    return df.select("region").toPandas()["region"].values.tolist()


def hottest_overall_date(df: DataFrame) -> DataFrame:
    """ determine the highest overall date temperature"""

    # preparing for analytic window functions to be applied against the data
    window1 = Window.partitionBy("ObservationDate")
    window2 = Window.orderBy(F.col("aggregated_date_temp").desc())
    df.repartition("ObservationDate")

    # sum all temperatures collected per date, then determine the highest
    hot = df.withColumn("aggregated_date_temp", F.sum("ScreenTemperature").over(window1)) \
        .withColumn("max_temp", F.row_number().over(window2)) \
        .filter(F.col("max_temp") == 1) \
        .select(*conf.overall_fields)
    return hot
