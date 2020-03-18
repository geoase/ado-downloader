#!/usr/bin/env python

"""
cds_downloader.py:
Climate Data Store Downloader

TODO:
 - Read JSON
 - Download yearly, monthly, or daily file
 - Check available data and only download missing data
 - Use api instead of json for filter
 - Logging
 - Use multiprocessing (or multiple docker instances) for big request
"""

__author__ = "Georg Seyerl"
__license__ = "MIT"
__maintainer__ = "Georg Seyerl"
__status__ = "Development"

from multiprocessing import Process
from pathlib import Path

import json
import cdsapi

class ClimateDataStoreBaseDownloader(object):
    """
    TODO
    """
    def __init__(self, storage_path, cds_product, cds_filter):
        self.storage_path = storage_path
        self.cds_product = cds_product
        self.cds_filter = cds_filter
        # FIXME User Credentials from client config file or .env
        self.cdsapi_client = cdsapi.Client()

    @classmethod
    def from_json(cls, json_config_path):
        try:
            #Read JSON config file
            with open(json_config_path, 'r') as f:
                cds_downloader = cls(**json.load(f))

            return cds_downloader
        except Exception as e:
            print(e.args)
            raise

    def get_data(self, input):
        """Method documentation"""
        raise NotImplementedError("Please Implement this method")

    def _retrieve_file(self, cds_product, cds_filter, file_name):
        self.cdsapi_client.retrieve(
            cds_product,
            cds_filter,
            file_name
        )


class YearlyDownloader(ClimateDataStoreBaseDownloader):
    def get_data(self):
        Path(self.storage_path).mkdir(parents=True, exist_ok=True)

        yearly_cds_filter = self.cds_filter
        lst_years = yearly_cds_filter.pop("year")

        for year in lst_years:
            yearly_cds_filter["year"] = year
            yearly_file_path = year + "_" + self.cds_product + "." + yearly_cds_filter.get("format", "grib")

            p = Process(
                target=self._retrieve_file,
                args=(self.cds_product, yearly_cds_filter, yearly_file_path)
            )
            p.start()
