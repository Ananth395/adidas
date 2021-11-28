import unittest
from pyspark.sql import SparkSession
from adidas.etl.run_sql import RunSql
from tests.etl.sql import __name__ as sql_dir


class ApplicationTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('testing'). \
            getOrCreate()
        # cls.log4j = cls.spark._jvm.org.apache.log4j
        # cls.log4j.LogManager.getLogger("ERROR")
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class RunSqlTestCase(ApplicationTestCase, RunSql):

    def test_read_sql_file(self):
        assert RunSql._read_sql_file(dir_name=sql_dir, task_name='test_sql', table_name='sql') == 'test sql'

    def test_create_temp_view(self):
        df = self.spark.createDataFrame(data=[[1, 'ananth']],
                                        schema=['id', 'name'])
        RunSql._create_temp_view(df, view_name='test_view')

        expected_df = self.spark.createDataFrame(data=[[1, 'ananth']],
                                                 schema=['id', 'name'])

        assert self.spark.sql('select * from test_view').collect() == expected_df.collect()

    def test_run_sql(self):
        sql = "select year(current_date) as year"
        df = self._run_sql(sql)
        expected_df = self.spark.createDataFrame(data=[[2021]],
                                                 schema=['year'])
        assert df.collect() == expected_df.collect()

    def test_read_orc_file(self):
        test_orc_dir = 'tests/parse_test/'
        df = self._read_orc_file(test_orc_dir)

        assert df.count() == 1
