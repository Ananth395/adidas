"""
Testing for helpers module
"""
import unittest

from adidas.utils.utils import parse_datetime, init_logger


class UtilsTestCase(unittest.TestCase):
    def test_parse_datetime(self):
        input_time = "1989"
        assert parse_datetime(input_time) == "1989-01-01"

        input_time = "05 1989"
        assert parse_datetime(input_time) == "1989-05-01"

        input_time = "March 10 1979"
        assert parse_datetime(input_time) == "1979-03-10"

        input_time = ""
        assert parse_datetime(input_time) == "1900-01-01"

        input_time = "abcd"
        assert parse_datetime(input_time) == "1900-01-01"

    def test_init_logger(self):
        test_logger = init_logger("log/test_debug.log")
        self.assertLogs(test_logger)
