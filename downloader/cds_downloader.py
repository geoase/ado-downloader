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

import os
import math
import json
import copy
import requests
import itertools

import cdsapi

class ClimateDataStoreDownloader(object):
    """
    TODO
    """
    def __init__(self, cds_product, cds_filter):
        self.cds_product = cds_product
        self.cds_filter = cds_filter
        self.cds_webapi = requests.get(
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

    def get_data(self, storage_path, split_keys=None):
        """Method documentation"""

        # Raise error if request is to big
        # Include description of split_keys argument
        request_size = self._get_request_size()
        selection_limit = self.cds_webapi["selection_limit"]

        Path(storage_path).mkdir(parents=True, exist_ok=True)

        if split_keys is None:
            file_path = 'all' + \
                        "_" + self.cds_product + \
                        "." + self.cds_filter.get("format", "grib")

            self._retrieve_file(self.cds_product,
                                self.cds_filter,
                                os.path.join(storage_path, file_path)
            )
        else:
            split_filter = self._expand_by_keys(self.cds_filter, split_keys)
            for cds_filter in split_filter:
                file_path = '-'.join([cds_filter.get(k) for k in split_keys]) + \
                            "_" + self.cds_product + \
                            "." + cds_filter.get("format", "grib")

                p = Process(
                    target=self._retrieve_file,
                    args=(self.cds_product,
                          cds_filter,
                          os.path.join(storage_path, file_path)
                    )
                )
                p.start()


    def _get_request_size(self, lst_keys=["variable", "year", "month", "day", "time"]):
        request_size = math.prod(
            [len(lst) for lst in [self.cds_filter.get(k) for k in lst_keys]]
        )
        return request_size


    def _expand_by_keys(self, dct, lst_keys):
        tmp_dct = copy.deepcopy(dct)
        for value in itertools.product(*[dct.get(key) for key in lst_keys]):
            tmp_dct.update(dict(zip(lst_keys, value)))
            yield tmp_dct


    def _retrieve_file(self, cds_product, cds_filter, file_name):
        self.cdsapi_client.retrieve(
            cds_product,
            cds_filter,
            file_name
        )
