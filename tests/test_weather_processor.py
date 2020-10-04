import datetime
from pyspark.sql import SparkSession, DataFrame
from app.config import get_spark_context
import app.app as app
import app.config as conf
import pytest
from app.weather_processor import (
highest_temp,
hottest_date,
hottest_temp,
hottest_region,
hottest_overall_date
)


class TestWeatherProcessor():

    def test_data(self, spark: SparkSession) -> DataFrame:
        """generate the test data"""
        dt1 = datetime.datetime.strptime('2020-06-29 08:15:27', '%Y-%m-%d %H:%M:%S')
        dt2 = datetime.datetime.strptime('2020-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        dt3 = datetime.datetime.strptime('2020-08-01 00:00:00', '%Y-%m-%d %H:%M:%S')

        test_data = [
            (1, dt1, 26, "london"),
            (5, dt2, 15, "manchester"),
            (12, dt3, 22, "essex"),
            (20, dt3, 20, "essex"),
        ]
        test_df  = spark.createDataFrame(test_data, conf.fields)

        return test_df


    def single_record_df(self, spark: SparkSession) -> DataFrame:
        """Test that the dataframe column ScreenTemperature is returned as list"""
        test_data = [(1, "'2020-06-29 08:15:27", 26, "london")]
        test_df = spark.createDataFrame(test_data, conf.fields)

        return test_df

    @pytest.mark.usefixtures("spark")
    def test_highest_temp(self, spark: SparkSession):
        dt1 = datetime.datetime.strptime('2020-06-29 08:15:27', '%Y-%m-%d %H:%M:%S')
        test_df = self.test_data(spark)
        expected_highest_temp = [(1, dt1, 26, "london")]
        expected_highest_temp_df = spark.createDataFrame(expected_highest_temp, conf.fields).collect()

        # Test that the record with the highest temperature is returned
        highest_temp_rec = highest_temp(test_df)
        assert highest_temp_rec.collect() == expected_highest_temp_df

        # Test that from the record with the highest temperature
        # the values for columns ObservationDate, ScreenTemperature
        # are returned as list
        highest_temp_list = hottest_date(highest_temp_rec)
        expected_list = ["2020-06-29 08:15:27", 1]
        assert highest_temp_list == expected_list

    @pytest.mark.usefixtures("spark")
    def test_hottest_temp(self, spark: SparkSession):
        """Test that the dataframe column ScreenTemperature is returned as list"""
        test_df = self.single_record_df(spark)
        actual = hottest_temp("ScreenTemperature", test_df)
        expected = [26]
        assert actual == expected


    @pytest.mark.usefixtures("spark")
    def test_hottest_region(self, spark: SparkSession):
        """Test that the dataframe column ScreenTemperature is returned as list"""
        test_df = self.single_record_df(spark)
        actual = hottest_region(test_df)
        expected = ["london"]
        assert actual == expected

    @pytest.mark.usefixtures("spark")
    def test_hottest_overall_date(self, spark: SparkSession):
        """Test the date with the overall hotest date"""
        dt = datetime.datetime.strptime('2020-08-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        test_df = self.test_data(spark)
        test_field = ["ObservationDate", "aggregated_date_temp", "region"]
        expected_hottest_overall_rec = [(dt, 42, "essex")]
        expected_hottest_overall_rec_df = spark.createDataFrame(expected_hottest_overall_rec, test_field).collect()

        # Test that the record with the highest temperature is returned
        hottest_overall_date_rec = hottest_overall_date(test_df).drop("ObservationTime")
        assert hottest_overall_date_rec.collect() == expected_hottest_overall_rec_df


