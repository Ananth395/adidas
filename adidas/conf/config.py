CONFIG = {
    "base_dir": "",
    "raw_dir": "raw/",
    "parse_partition_col": "load_date",
    "parse_dir": "parse/",
    "log_file": "log/debug.log",
    "filter_on_publish_date": 1950,
    "filter_on_number_of_pages": 20,
    "parsed_table_name": "p_books_data",
    "cil_dir": "cil/",
    "sql_dir": "etl.sql",
}


# list of etl tasks
etl_tasks = ["raw_to_parse"]


# list of sql tasks
sql_tasks = [
    "get_top_5_genres",
    "get_book_with_most_pages",
    "get_no_of_published_books_and_authors",
    "get_no_of_authors_with_one_book_per_yr",
    "get_top_5_coauthors",
]
