from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class ETLBase(ABC):
    """
    Base class to Extract, Transform and Load datasets
    """

    def __init__(self, logger):
        self.logger = logger

    @abstractmethod
    def _extract(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _load(self, df: DataFrame) -> None:
        raise NotImplementedError

    def run_process(self) -> None:
        try:
            self.logger.info("Starting main process")
            df_input = self._extract()
            df_transformed = self._transform(df_input)
            self._load(df_transformed)
            self.logger.info("Completed main process")
        except Exception as e:
            self.logger.error("Exiting main process")
            raise Exception from e
