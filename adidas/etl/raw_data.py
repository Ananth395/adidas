import os
import sys
from pathlib import Path

import wget

from adidas.conf import config
from adidas.utils.utils import init_logger


class RawData:
    """
    Class to download raw data file from http servers
    """

    def __init__(self, url):
        self.url = url
        self.out_dir = config.CONFIG["raw_dir"]
        self.logger = init_logger(config.CONFIG["log_file"])
        self.file_name = self.url.split("/")[-1]

    def download(self) -> None:
        """
        Helps to download data files
        :return: None
        """
        try:
            self.logger.info(f"Starting to download file:{self.file_name}")
            self._create_if_dir_exists(self.out_dir)
            if os.path.exists(self.out_dir + "/" + self.file_name):
                os.remove(self.out_dir + "/" + self.file_name)

            wget.download(self.url, out=self.out_dir, bar=self._bar_progress)

            if os.path.exists(self.out_dir + "/" + self.file_name):
                self.logger.info(f"file:{self.file_name} successfully downloaded")
        except Exception as e:
            self.logger.error(f"Failed to download file:{self.file_name}")
            self.logger.exception(e)
            raise Exception from e

    @staticmethod
    def _bar_progress(current, total, width=80):
        """
        creates progress bar while file is downloading
        :param current:
        :param total:
        :param width:
        :return:
        """
        progress_message = (
            f"Downloading: {(current / total * 100)}% [{current}/ {total}] bytes"
        )
        sys.stdout.write("\r" + progress_message)
        sys.stdout.flush()

    @staticmethod
    def _create_if_dir_exists(dir_to_check):
        """
        creates directory if not exists
        :param dir_to_check: name of the directory
        :return: None
        """
        if not Path(dir_to_check).exists():
            Path(dir_to_check).mkdir(parents=True, exist_ok=True)
