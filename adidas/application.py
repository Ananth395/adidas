from logging import log

import click
from pyspark.sql import SparkSession

from adidas.conf import config as settings
from adidas.etl.raw_to_parse import RawToParse
from adidas.etl.run_sql import RunSql
from adidas.utils.utils import init_logger


def start_spark(app_name):
    """
    starts spark session for the application
    :param app_name:
    :return:
    """
    spark: SparkSession = SparkSession.builder.appName(app_name).getOrCreate()

    # log4j = spark._jvm.org.apache.log4j
    # logger = log4j.LogManager.getLogger(app_name)

    return spark


def _check_if_etl_task_exists(task_name) -> bool:
    """
    checks if the etl task is valid
    :param task_name: name of the task to be validated
    :return: bool
    """
    return task_name in settings.etl_tasks


def _check_if_sql_task_exists(task_name) -> bool:
    """
    checks if the sql task is valid
    :param task_name: name of the task to be validated
    :return: bool
    """
    return task_name in settings.sql_tasks


@click.command()
@click.option(
    "--task_type",
    required=True,
    help="type of task: etl or sql",
    type=click.Choice(["etl", "sql"]),
)
@click.option("--task_name", required=True, help="name of the task to be run")
@click.option("--write_to_file", type=bool)
def main(task_type, task_name, write_to_file):
    """
    Application's entry point
    """
    spark = None
    logger: log = init_logger(settings.CONFIG["log_file"])
    try:
        spark: SparkSession = start_spark("adidas_case_study")
        app_id: str = spark.sparkContext.applicationId
        logger.info(f"Starting Spark Application with id: {app_id}")
        if task_type.lower() == "etl":  # and
            if not _check_if_etl_task_exists(task_name.lower()):
                logger.error("Invalid task name. Check documentation")
                raise Exception
            if task_name.lower() == "raw_to_parse":
                RawToParse(spark, logger, settings).run_process()

        elif task_type.lower() == "sql":
            if not _check_if_sql_task_exists(task_name.lower()):
                logger.error("Invalid task name. Check documentation")
                raise Exception
            RunSql(
                spark=spark,
                task_name=task_name.lower(),
                logger=logger,
                setting=settings.CONFIG,
                write_to_file=write_to_file,
            ).run_sql()

        logger.info(f"Completed Spark Application id: {app_id}")
    except Exception as e:
        logger.error("Exiting application")
        logger.exception(e)
        raise Exception from e
    finally:
        logger.info("Stopping SparkSession")
        spark.stop()
        del spark
        del logger
