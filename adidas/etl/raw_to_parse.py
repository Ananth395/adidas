from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from adidas.conf import config
from adidas.etl.etl_base import ETLBase
from adidas.utils.utils import parse_datetime


class RawToParse(ETLBase):
    """
    Class helps to read, process and load data from raw to parse layer
    """

    def __init__(self, spark, logger, settings):
        super().__init__(logger)
        self.spark: SparkSession = spark
        self.logger = logger
        self.settings = settings

    def _extract(self) -> DataFrame:
        try:
            return self._read_json_file(config.CONFIG["raw_dir"])
        except Exception as e:
            self.logger.error("error while reading data json file")
            self.logger.exception(e)
            raise Exception from e

    def _transform(self, df: DataFrame) -> DataFrame:
        try:
            self.logger.info("enriching data from raw")

            df.cache()
            df = self._remove_nulls(df, column_name="title")
            df = self._filter_data(
                df,
                column_name="number_of_pages",
                val=self.settings.CONFIG["filter_on_number_of_pages"],
            )
            df = self._normalize_publish_date(df)

            df = self._filter_data(
                df,
                column_name="publish_year",
                val=self.settings.CONFIG["filter_on_publish_date"],
            )

            df = self._add_load_date(df, self.settings.CONFIG["parse_partition_col"])

            return df
            # return \
            #     self._add_load_date(
            #         self._filter_data(
            #             self._remove_nulls(
            #                 self._filter_data(
            #                     self._remove_nulls(df, column_name='publish_date'),
            #                     column_name='publish_date', val=self.settings.CONFIG["filter_on_publish_date"]),
            #                 column_name="title"),
            #             column_name="number_of_pages", val=self.settings.CONFIG["filter_on_number_of_pages"]),
            #         self.settings.CONFIG["parse_partition_col"]
            #     )
            # return self._add_load_date(df, self.settings.CONFIG["parse_partition_col"])
        except Exception as e:
            self.logger.error("error while enriching data for stage")
            self.logger.exception(e)
            raise Exception from e

    def _load(self, df: DataFrame) -> None:
        try:
            self.logger.info("loading data into parse")
            part_num = self.spark.sparkContext.defaultParallelism
            if df.rdd.getNumPartitions() > part_num:
                df = df.coalesce(part_num)
            # df.persist(StorageLevel.MEMORY_AND_DISK)
            df.write.partitionBy(self.settings.CONFIG["parse_partition_col"]).mode(
                "overwrite"
            ).orc(self.settings.CONFIG["parse_dir"])

            # psuedo code to save dataframe as table
            # if self.table_exists(target_db, table):
            #     self.drop_seq_partitions(sch_name, seq)
            #     df.write.format("orc").insertInto(table_name)
            # else:
            #     df.write.saveAsTable(table_name, "orc", "append", "load_date")
        except Exception as e:
            self.logger.error("error while writing data in parse")
            self.logger.exception(e)
            raise Exception from e

    @staticmethod
    def _add_load_date(df: DataFrame, partition_col_name: str) -> DataFrame:
        """
        Appends current_date as new column for audit purpose
        :param df: Dataframe
        :param partition_col_name: column name
        :return: Stage Dataframe
        """
        return df.withColumn(partition_col_name, F.current_date())

    @staticmethod
    def _filter_data(df: DataFrame, column_name: str, val: int) -> DataFrame:
        """
        removes null values from column
        :param df:
        :return:
        """
        return df.where(col(f"{column_name}") > val)

    @staticmethod
    def _remove_nulls(df: DataFrame, column_name: str) -> DataFrame:
        """
        removes null values from column
        :param df:
        :return:
        """
        return df.where(col(f"{column_name}").isNotNull())

    @staticmethod
    def _normalize_publish_date(df: DataFrame) -> DataFrame:
        """
        Helps to normalize publish_date column
        :param df: dataframe
        :return: Dataframe
        """
        df = df.withColumn("new_publish_date", parse_datetime(col("publish_date")))
        df = (
            df.withColumn(
                "publish_year", F.split(df["new_publish_date"], "-").getItem(0)
            )
            .withColumn(
                "publish_month", F.split(df["new_publish_date"], "-").getItem(1)
            )
            .withColumn("publish_day", F.split(df["new_publish_date"], "-").getItem(2))
        )
        return df

    def _read_json_file(self, dir_name) -> DataFrame:
        """
        helps to read json data files
        :param dir_name: name of the directory containing the files
        :return: Dataframe
        """
        return self.spark.read.json(dir_name)
