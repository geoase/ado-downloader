#!/usr/bin/env python

"""
cds_downloader.py:
Climate Data Store Downloader

TODO:
 - Tests
 - Check available data and only download missing data
 - Logging
 - Adapt requirements.txt
 - Dynamic split by lists in cds_filter (order) e.g.
   ensemble_member, experiment, model
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
    def from_cds(cls, cds_product, cds_filter):
        try:
            dct_config = {"cds_product": cds_product,
                          "cds_filter": cds_filter}
            cds_downloader = cls(**dct_config)

            return cds_downloader
        except Exception as e:
            print(e.args)
            raise

    @classmethod
    def from_dict(cls, dct_config):
        try:
            cds_downloader = cls(**dct_config)

            return cds_downloader
        except Exception as e:
            print(e.args)
            raise

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
        if split_keys is None:
            split_keys = self._get_split_keys()

        Path(storage_path).mkdir(parents=True, exist_ok=True)

        split_filter = self._expand_by_keys(self.cds_filter, split_keys)
        for cds_filter in split_filter:
            file_path = '-'.join([cds_filter.get(k) for k in split_keys] or ["all"]) + \
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


    def _get_split_keys(self, lst_keys = ["year", "month", "day", "time", "variable"]):
        if self._get_request_size(lst_keys) < self.cds_webapi["selection_limit"]:
            return list()
        else:
            k = lst_keys.pop(0)
            split_keys = self._get_split_keys(lst_keys)
            split_keys.insert(0, k)
            return split_keys


    def _get_request_size(self, lst_keys):
        request_size = math.prod(
            [len(lst) for lst in [self.cds_filter.get(k, 1) for k in lst_keys]]
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
