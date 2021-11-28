from typing import Callable

from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class UDFHelper:
    """
    Used for decorating python function as pyspark
    """

    def __init__(self, returnType: StringType()):
        self.spark_udf_type = returnType

    def __call__(self, func: Callable):
        def wrapped_func(*args, **kwargs):
            if any([isinstance(arg, Column) for arg in args]) or any(
                [isinstance(vv, Column) for vv in kwargs.values()]
            ):
                return udf(func, self.spark_udf_type)(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        return wrapped_func
