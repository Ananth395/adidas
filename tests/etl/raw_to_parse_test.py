"""
Testing for Raw_to_parse module
"""

import unittest
from pyspark.sql import SparkSession
from adidas.etl.raw_to_parse import RawToParse
from datetime import date, timedelta


class ApplicationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("testing").getOrCreate()
        # cls.log4j = cls.spark._jvm.org.apache.log4j
        # cls.log4j.LogManager.getLogger("ERROR")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class RawToParseTestCase(ApplicationTestCase, RawToParse):
    def test_add_load_date(self):
        df = self.spark.createDataFrame(data=[[2, "ananth"]], schema=["id", "name"])

        # date(2021, 11, 28)]
        expected_df = self.spark.createDataFrame(
            data=[[2, "ananth", date.today()]], schema=["id", "name", "load_date"]
        )

        assert self._add_load_date(df, "load_date").collect() == expected_df.collect()

        df = self.spark.createDataFrame(data=[[2, "ananth"]], schema=["id", "name"])

        # date(2021, 11, 27)]
        not_expected_df = self.spark.createDataFrame(
            data=[[2, "ananth", date.today() - timedelta(days=1)]],
            schema=["id", "name", "load_date"],
        )

        assert (
            self._add_load_date(df, "load_date").collect() != not_expected_df.collect()
        )

    def test_filter_data(self):
        df = self.spark.createDataFrame(
            data=[[1, "abcd"], [2, "ananth"]], schema=["id", "name"]
        )
        expected_df = self.spark.createDataFrame(
            data=[[2, "ananth"]], schema=["id", "name"]
        )

        assert (
            self._filter_data(df, column_name="id", val=1).collect()
            == expected_df.collect()
        )

        df = self.spark.createDataFrame(
            data=[[1, "abcd"], [2, "ananth"]], schema=["id", "name"]
        )
        not_expected_df = self.spark.createDataFrame(
            data=[[2, "ananth"]], schema=["id", "name"]
        )

        assert (
            self._filter_data(df, column_name="id", val=2).collect()
            != not_expected_df.collect()
        )

    def test_remove_nulls(self):
        df = self.spark.createDataFrame(
            data=[[1, None], [2, "ananth"]], schema=["id", "name"]
        )
        expected_df = self.spark.createDataFrame(
            data=[[2, "ananth"]], schema=["id", "name"]
        )

        assert (
            self._remove_nulls(df, column_name="name").collect()
            == expected_df.collect()
        )

        df = self.spark.createDataFrame(
            data=[[1, "NULL"], [2, "ananth"]], schema=["id", "name"]
        )
        not_expected_df = self.spark.createDataFrame(
            data=[[2, "ananth"]], schema=["id", "name"]
        )

        assert (
            self._remove_nulls(df, column_name="name").collect()
            != not_expected_df.collect()
        )

    def test__normalize_publish_date(self):
        df = self.spark.createDataFrame(
            data=[
                ["1989"],
                ["March 10 1979"],
                ["abcd"],
            ],
            schema=["publish_date"],
        )

        expected_df = self.spark.createDataFrame(
            data=[
                ["1989", "1989-01-01", "1989", "01", "01"],
                ["March 10 1979", "1979-03-10", "1979", "03", "10"],
                ["abcd", "1900-01-01", "1900", "01", "01"],
            ],
            schema=[
                "publish_date",
                "new_publish_date",
                "publish_year",
                "publish_month",
                "publish_day",
            ],
        )

        assert self._normalize_publish_date(df).collect() == expected_df.collect()

    def test_read_json_file(self):
        test_json_dir = "tests/raw_test/"
        df = self._read_json_file(test_json_dir)

        assert df.count() == 2
