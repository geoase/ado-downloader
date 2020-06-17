#!/usr/bin/env python

"""
cds_downloader.py:
Climate Data Store Downloader

TODO:
 - Download yearly, monthly, or daily file
 - Check available data and only download missing data
 - Use api instead of json for filter (difficult constraints)
 - Logging
 - Adapt requirements.txt
"""

__author__ = "Georg Seyerl"
__license__ = "MIT"
__maintainer__ = "Georg Seyerl"
__status__ = "Development"

from multiprocessing import Process
from pathlib import Path

import json
import cdsapi
import os
import requests

class ClimateDataStoreBaseDownloader(object):
    """
    TODO
    """
    def __init__(self, cds_product, cds_filter):
        self.cds_product = cds_product
        self.cds_filter = cds_filter

        # TODO Exception handling
        self.cds_api = requests.get(
            url='https://cds.climate.copernicus.eu/api/v2.ui/resources/{}'.format(cds_product)).json()

        # User Credentials from environment variables 'CDSAPI_URL' and 'CDSAPI_KEY'
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

    def _get_request_size(self):
        request_size = (len(self.cds_filter["variable"])*
                        len(self.cds_filter["year"])*
                        len(self.cds_filter["month"])*
                        len(self.cds_filter["day"])*
                        len(self.cds_filter["time"]))

        return request_size


class YearlyDownloader(ClimateDataStoreBaseDownloader):
    def get_data(self, storage_path):
        Path(storage_path).mkdir(parents=True, exist_ok=True)

        yearly_cds_filter = self.cds_filter
        lst_years = yearly_cds_filter.pop("year")

        for year in lst_years:
            yearly_cds_filter["year"] = year
            yearly_file_path = year + "_" + self.cds_product + "." + yearly_cds_filter.get("format", "grib")

            p = Process(
                target=self._retrieve_file,
                args=(self.cds_product,
                      yearly_cds_filter,
                      os.path.join(storage_path, yearly_file_path)
                )
            )
            p.start()


class MonthlyDownloader(ClimateDataStoreBaseDownloader):
    def get_data(self, storage_path):
        Path(storage_path).mkdir(parents=True, exist_ok=True)

        monthly_cds_filter = self.cds_filter
        lst_years = monthly_cds_filter.pop("year")
        lst_months = monthly_cds_filter.pop("month")

        for year in lst_years:
            monthly_cds_filter["year"] = year
            for month in lst_months:
                monthly_cds_filter["month"] = month
                monthly_file_path = year + month + "_" + self.cds_product + "." + monthly_cds_filter.get("format", "grib")

                p = Process(
                    target=self._retrieve_file,
                    args=(self.cds_product,
                          monthly_cds_filter,
                          os.path.join(storage_path, monthly_file_path)
                    )
                )
                p.start()


class DailyDownloader(ClimateDataStoreBaseDownloader):
    def get_data(self, storage_path):
        Path(storage_path).mkdir(parents=True, exist_ok=True)

        daily_cds_filter = self.cds_filter
        lst_years = daily_cds_filter.pop("year")
        lst_months = daily_cds_filter.pop("month")
        lst_days = daily_cds_filter.pop("day")

        for year in lst_years:
            daily_cds_filter["year"] = year
            for month in lst_months:
                daily_cds_filter["month"] = month
                for day in lst_days:
                    daily_cds_filter["day"] = day
                    daily_file_path = year + month + day + "_" + self.cds_product + "." + daily_cds_filter.get("format", "grib")

                    p = Process(
                        target=self._retrieve_file,
                        args=(self.cds_product,
                              daily_cds_filter,
                              os.path.join(storage_path, daily_file_path)
                        )
                    )
                    p.start()
