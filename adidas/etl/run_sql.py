from typing import Optional

import pkg_resources
from pyspark.sql import DataFrame, SparkSession

from adidas.etl.sql import __name__ as sql_dir


class RunSql:
    """
    This class helps to run sql tasks using pyspark
    """

    app_name = "run_sql_on_parse"

    def __init__(
        self, spark, logger, setting, task_name, write_to_file=False, show_output=True
    ):
        self.spark: SparkSession = spark
        self.setting = setting
        self.logger: Optional = logger
        self.task_name = task_name
        self.write_to_file = write_to_file
        self.show_output = show_output

    def run_sql(self) -> None:
        """
        Function to read the sql file, run and save the output dataframe
        :return:
        """

        try:
            self.logger.info(f"Starting to execute sql-task_name:{self.task_name}")
            sql = self._read_sql_file(
                dir_name=sql_dir,
                task_name=self.task_name,
                table_name=self.setting["parsed_table_name"],
            )
            # df_parse = self.spark.read.orc(self.setting['parse_dir'])
            df_parse = self._read_orc_file(self.setting["parse_dir"])
            # df_parse.createOrReplaceTempView(f"{self.setting['parsed_table_name']}")
            self._create_temp_view(
                df_parse, view_name=self.setting["parsed_table_name"]
            )
            df_out = self._run_sql(sql)
            if self.write_to_file:
                df_out.repartition(1).write.csv(
                    self.setting["cil_dir"] + "/" + self.task_name, header=True
                )
            if self.show_output:
                df_out.show(20, False)
            self.logger.info(f"Completed execution of sql-task_name:{self.task_name}")
        except Exception as e:
            self.logger.error(f"Error while running sql-task_name:{self.task_name}")
            self.logger.exception(e)
            raise Exception from e

    def _run_sql(self, sql) -> DataFrame:
        """
        Helps to run spark sql
        :param sql: sql query
        :return: Dataframe
        """
        return self.spark.sql(sql)

    @staticmethod
    def _create_temp_view(df: DataFrame, view_name) -> None:
        """
        Helps to create temporary view on Dataframe
        :param df: Dateframe
        :param view_name: name of the temporary view
        :return: None
        """
        df.createOrReplaceTempView(view_name)

    def _read_orc_file(self, dir_name) -> DataFrame:
        """
        Helps to read orc data files
        :param dir_name: name of the directory containing orc files
        :return: Dataframe
        """
        return self.spark.read.orc(dir_name)

    @staticmethod
    def _read_sql_file(dir_name, task_name, table_name) -> str:
        """
        Helps to read the query from the .sql files
        :param dir_name: name of the directory containing .sql files
        :param task_name: name of the task to be run
        :param table_name: table name in parsed layer
        :return: sql query
        """
        sql_file = task_name + ".sql"
        sql_template = (
            pkg_resources.resource_string(dir_name, sql_file)
            .decode()
            .strip()
            .format(table_name=table_name)
        )
        return sql_template
