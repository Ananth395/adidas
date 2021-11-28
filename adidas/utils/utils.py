import logging
from datetime import date

from dateutil.parser import ParserError, parse
from pyspark.sql.types import DateType, StringType

from adidas.utils.udfhelper import UDFHelper


def init_logger(log_file_name):
    """
    function which starts the logger
    :param log_file_name: name of the log file
    :return: logger
    """
    logger = logging.getLogger()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file_name), logging.StreamHandler()],
    )

    return logger


@UDFHelper(returnType=StringType())
def parse_datetime(val: str) -> str:
    """
    Parses string and generated datetime
    :param val: datetime string
    :return: parsed datetime string
    """
    try:
        if val is not None:
            dt = parse(val, default=date(1900, 1, 1), fuzzy=True, ignoretz=True)
            return dt.strftime("%Y-%m-%d")
    except ParserError:
        return "1900-01-01"
