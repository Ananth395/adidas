# Set shell to use bash
SHELL := /bin/bash -o pipefail -o errexit -x

# Main package name for tests, linting etc.
APP_PGK_NAME="adidas"
TEST_PGK_NAME="tests"

help: ## Lists available commands and their explanation
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

run-dev-setup:
	pip install poetry
	poetry install

run-build:
	poetry run python setup.py clean bdist_egg

run-update-dependencies:
	poetry update

run-type-checking: ## Run static type checking on the code
	poetry run mypy $(APP_PGK_NAME)

run-linting: ## Run source code linting using pylint
	poetry run pylint $(APP_PGK_NAME)

run-unittests: ## Run unittests
	poetry run pytest $(TEST_PGK_NAME)

run-black: ## Format code with black
	poetry run black .

run-check-black: ## Check if the style of the code conforms to the desired style
	# First print the difference to the desired style
	poetry run black --diff $(APP_PGK_NAME)/

	# Second print the list of files which would have changed
	poetry run black --check $(APP_PGK_NAME)/

run-isort: ## Apply import sorting
	poetry run isort $(APP_PGK_NAME)/

run-check-isort: ## Check if imports are sorted
	poetry run isort $(APP_PGK_NAME)/ --diff
	isort $(APP_PGK_NAME)/ --check-only

run-format-code: ## Format the code according to the required style
	$(MAKE) run-black
	$(MAKE) run-isort

run-validation-tasks: ## Run all validation tasks in sequence, fail on first error
	## Run all validation tasks in sequence
	echo ######################### [ run-type-checking ] #########################
	$(MAKE) run-type-checking

	echo ######################### [ run-check-code-stye ] #########################
	$(MAKE) run-check-black
	$(MAKE) run-check-isort

	echo ######################### [ run-linting ] #########################
	$(MAKE) run-linting

	echo ######################### [ run-unittests ] #########################
	$(MAKE) run-unittests

	@echo
	@echo All validation tasks passed ! Awesome !
	@echo --

url = https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json
run-get-raw-data:
	poetry run python \
	get_raw_data.py --url=$(url)

task_type = etl
task_name = raw_to_parse

run-spark-local-etl:
	poetry run spark-submit \
	--master local[*] \
	main.py --task_type=etl --task_name $(task_name)


run-spark-local-sql:
	poetry run spark-submit \
	--master local[*] \
	main.py --task_type=sql --task_name=get_top_5_coauthors #--write_to_file=True

# Spark configuration
EXECUTORS="14"
DRIVER_CORES="1"
EXEC_CORES="2"
DRIVER_MEMORY="10G"
EXEC_MEMORY="14G"
PARTITIONS="200"

run-task-cluster:
	poetry run spark-submit --master yarn \
	--deploy-mode cluster \
	--executor-memory "$(EXEC_MEMORY)" \
	--executor-cores "$(EXEC_CORES)" \
	--num-executors "$(EXECUTORS)" \
	--driver-memory "$(DRIVER_MEMORY)" \
	--driver-cores "$(DRIVER_CORES)" \
	--conf "spark.pyspark.python=/opt/rh/rh-python38/root/bin/python" \
	--conf "spark.sql.shuffle.partitions=$(PARTITIONS)" \
	--conf "spark.sql.hive.filesourcePartitionFileCacheSize=1073741824" \
	--py-files "Ananth395-data-engineering-test/dist/hellofresh-0.1.0-py3.8.egg" \
	main.py --task_type=$(task_type) --task_name $(task_name)