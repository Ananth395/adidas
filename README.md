# Adidas Data Engineering Test

The project directory is under `/adidas`

Following is the project's structure:
* **main.py**: The `python` requires this file to trigger the process
* **adidas** : The project's source code
* **raw** : file directory for `raw` layer
* **parse** : file directory for `parse` layer
* **cil** : file directory for `cil` layer
* **log** : Application's log directory
* **tests** : The Project's unit test sources
* **pyproject.toml** : This file maintains the project dependencies

## 1. Requirements

* Python 3.8
* poetry : Python packaging and dependency management
* apache-spark : Parallel Data Processing engine
* apache-airflow : Distributed Scheduler
* black : Python code formatter
* mypy : Python static type checker
* isort : Python package sorter
* pytest : Python Testing tool

## 2. Setup

Navigate to the [makefile](Makefile) and run the below command:
```
run-dev-setup
```
## 3. Running Unit Tests
Navigate to the [makefile](Makefile) and run the below command:
```
run-unittests
```
## 4. Running the application
Navigate to the [makefile](Makefile) and run the below command:

`to download raw data`
```
run-get-raw-data
```
Change the `task_name` variable. values = `['Task1', 'Task2'] `

`To run etl task in spark local`
```
run-spark-local-etl
```
`to run sql task in spark local `
```
run-spark-local-sql
```
`for spark cluster`
```
run-task-cluster
```

For more information, type below command
```
python main.py --help
```
## 5. Data Layers
####[`raw` -> `parse` -> `cil`]
* `raw` : Contains raw data in json format.
* `parse` : Contains clean data with some pre-processing in orc format.
* `cil`: Common Information Layer contains transformed data after applying business logic in csv format.

## 6. Tasks

* `get_raw_data` : downloads data file into raw layer.

* `task_type:etl` [`raw` -> `parse`]
    * `task_name: raw_to_parse`: ingests data from `raw` and creates `parse` 
  
* `task_type:sql` [`parse` -> `cil`]
    * `task_name: get_top_5_genres`: Find the top 5 genres with most books.
    * `task_name: get_book_with_most_pages`: Get the book with the most pages.
    * `task_name: get_no_of_published_books_and_authors`: Find the number of authors and number of books published per month for years between 1950 and 1970.
    * `task_name: get_no_of_authors_with_one_book_per_yr`: Per publish year, get the number of authors that published at least one book.
    * `task_name: get_top_5_coauthors` : Retrieve the top 5 authors who (co-)authored the most books.


`Note: sql is dependent on etl`

## 7. Adding new sql tasks
* `step 1` : prepare the sql and save it as `<task_name>.sql`.
* `step 2` : place the sql file created into `/etl/sql` directory.
* `step 3`: make an entry in the sql_tasks in `config.py`.
* `step 4`: pass `<task_name>` to `--task_name` argument in spark_submit.
* `step 5`: rebuild the egg file by running `run-build` in the [makefile](Makefile) (to run in cluster)

## 8. Scheduling
Apache Airflow can be used for scheduling. 
Please see the code [here](dags/adidas_airflow_job.py)

## 9. CI
Github actions can be used for CI.
A sample CI pipeline config can be seen [here](.github/workflows/ci.yml)

## 10. Python packaging and dependency management
Poetry is used for Python packaging and dependency management.
