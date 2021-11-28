from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

ETL_CODE_LOCATION = "s3://adidas-etl-binaries/adidas-etl/latest"

default_args = {
    "owner": "ananth395",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 29),
    "email": ["ananth395@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "end_date": datetime(2022, 11, 29),
}


def _python_operator(url: str) -> BashOperator:
    return BashOperator(
        env={"PATH": "/bin:/usr/bin:/usr/local/bin"},
        task_id="get_raw_data",
        bash_command=f"""
        python
        get_raw_data.py --url=${url}
        """,
    )


def _spark_submit_operator(task_type: str, task_name: str) -> BashOperator:
    return BashOperator(
        env={"PATH": "/bin:/usr/bin:/usr/local/bin"},
        task_id=task_type + "_" + task_name,
        bash_command=f"""
        spark-submit \
        --conf "spark.pyspark.python=/opt/rh/rh-python38/root/bin/python" \
        --conf "spark.sql.shuffle.partitions=200" \
        --conf "spark.sql.hive.filesourcePartitionFileCacheSize=1073741824" \
        --py-files "adidas/dist/adidas-0.1.0-py3.8.egg" \
        f"{ETL_CODE_LOCATION}/main.py --task_type=${task_type} --task_name ${task_name}
        """,
    )


with DAG(
    "adidas_airflow_job", default_args=default_args, schedule_interval=timedelta(1)
) as dag:
    print_date = BashOperator(task_id="print_date", bash_command="date")
    get_raw_data = _python_operator(
        url="https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump1.json"
    )

    etl_raw_to_parse = _spark_submit_operator(task_type="etl", task_name="raw_to_parse")

    sql_get_top_5_genres = _spark_submit_operator(
        task_type="sql", task_name="get_top_5_genres"
    )
    sql_get_book_with_most_pages = _spark_submit_operator(
        task_type="sql", task_name="get_book_with_most_pages"
    )
    sql_get_no_of_published_books_and_authors = _spark_submit_operator(
        task_type="sql", task_name="get_no_of_published_books_and_authors"
    )
    sql_get_no_of_authors_with_one_book_per_yr = _spark_submit_operator(
        task_type="sql", task_name="get_no_of_authors_with_one_book_per_yr"
    )
    sql_get_top_5_coauthors = _spark_submit_operator(
        task_type="sql", task_name="get_top_5_coauthors"
    )

    print_date >> get_raw_data >> etl_raw_to_parse

    etl_raw_to_parse >> sql_get_top_5_genres
    etl_raw_to_parse >> sql_get_book_with_most_pages
    etl_raw_to_parse >> sql_get_no_of_published_books_and_authors
    etl_raw_to_parse >> sql_get_no_of_authors_with_one_book_per_yr
    etl_raw_to_parse >> sql_get_top_5_coauthors
