#!/usr/bin/env python

"""
cds_downloader.py:
Climate Data Store Downloader

TODO:
 - Check available data and only download missing data
 - Logging
 - Adapt requirements.txt
 - If config not complete, use criterias from cds_webapi
 - Dynamic split by lists in cds_filter (order) e.g.
   ensemble_member, experiment, model
 - Check configuration
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
import datetime
import cdsapi
import pandas
import numpy

import shutil
import glob
import subprocess
import tempfile
import xarray


class ClimateDataStoreDownloader(object):
    """
    TODO
    """
    def __init__(self, cds_product, cds_filter, **kwargs):
        self.cds_product = cds_product
        self.cds_filter = cds_filter
        self.cds_webapi = requests.get(
            url='https://cds.climate.copernicus.eu/api/v2.ui/resources/{}'.format(cds_product)).json()

        # User Credentials from environment variables
        # 'CDSAPI_URL' and 'CDSAPI_KEY'
        self.cdsapi_client = cdsapi.Client()


    @classmethod
    def from_cds(cls, cds_product, cds_filter, **kwargs):
        try:
            dct_config = {"cds_product": cds_product,
                          "cds_filter": cds_filter}
            dct_config.update(**kwargs)
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
        # Create storage path
        Path(storage_path).mkdir(parents=True, exist_ok=True)

        # If necessary, find keys for download chunking
        if split_keys is None:
            split_keys = self._get_split_keys()

        split_filter = self._expand_by_keys(self.cds_filter, split_keys)
        all_processes = []
        for cds_filter in split_filter:
            file_path = '_'.join([cds_filter.get(k) for k in split_keys] or ["all"]) + \
                        "_" + self.cds_product + \
                        "." + cds_filter.get("format", "grib")

            all_processes.append(
                Process(
                    target=self._retrieve_file,
                    args=(self.cds_product,
                          cds_filter,
                          os.path.join(storage_path, file_path)
                    )
                )
            )

        for p in all_processes:
            p.start()
        for p in all_processes:
            p.join()

        return all_processes

    def _get_org_keys(self):
        exclude_keys = ["area"]
        lst_org = [k for k,v in self.cds_filter.items() if isinstance(v, list) and k not in exclude_keys]
        return lst_org

    def _get_request_size(self, lst_keys):
        request_size = math.prod(
            [len(lst) for lst in [self.cds_filter.get(k, 1) for k in lst_keys]]
        )
        return request_size

    def _get_split_keys(self):
        lst_org = self._get_org_keys()
        lst_ret = list()
        while self._get_request_size(lst_org) > self.cds_webapi["selection_limit"]:
            lst_ret.append(lst_org.pop(0))
        return lst_ret

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




class ClimateDataStoreUpdater(ClimateDataStoreDownloader):
    """
    TODO
    Update data collection, using information from cds metadata webapi. Redownload latest file to guarantee
    Under development, for now only temporal update
    """

    def update_data(self, path_string, split_keys, date_until=datetime.datetime.utcnow(), start_from_files=False):
        # TODO:
        # - split_keys includes non temporal attributes (e.g. variable)

        path_storage = Path(path_string)

        if path_storage.is_dir():
            lst_existing_files = [f for f in path_storage.glob("*." + self.cds_filter.get("format", "grib"))]
            lst_existing_files = sorted(lst_existing_files)
        else:
            raise("No valid path")

        temporal_filter = self._full_time_filter_from_webapi()

        file_split_keys = [tuple(f.name.rsplit("_")[:len(split_keys)]) for f in lst_existing_files]
        all_split_keys = [i for i in itertools.product(
            *[dict(self.cds_filter, **temporal_filter)[k] for k in split_keys])
        ]

        # Until present date
        index_present = all_split_keys.index(
            tuple(str(date_until.__getattribute__(k)).zfill(2) for k in split_keys if k in temporal_filter.keys())
        )

        # Include last tuple
        missing_split_keys = [keys for keys in all_split_keys[:index_present+1] if keys not in file_split_keys[:-1]]

        # Exclude dates earlier than date of first file
        if start_from_files:
            index_first = all_split_keys.index(file_split_keys[0])
            missing_split_keys = [keys for keys in all_split_keys[index_first:index_present+1] if keys not in file_split_keys[:-1]]


        # Download new data in temporary folder
        with tempfile.TemporaryDirectory() as temporary_path:
            dct_update = [{k:v for k,v in zip(split_keys, missing_split_key)} for missing_split_key in missing_split_keys]
            split_filter = (dict(self.cds_filter, **upd) for upd in dct_update)

            all_processes = []
            for cds_filter in split_filter:
                file_path = '_'.join([cds_filter.get(k) for k in split_keys] or ["all"]) + \
                            "_" + self.cds_product + \
                            "." + cds_filter.get("format", "grib")

                all_processes.append(
                    Process(
                        target=self._retrieve_file,
                        args=(self.cds_product,
                            cds_filter,
                            os.path.join(temporary_path, file_path)
                        )
                    )
                )

            for p in all_processes:
                p.start()
            for p in all_processes:
                p.join()


            lst_new_files = [f for f in Path(temporary_path).iterdir()]

            # Move files from tmp folder to storage path
            for f in lst_new_files:
                # Move and overwrite merged file
                shutil.move(f, path_storage.joinpath(f.name))


    def _full_time_filter_from_webapi(self, filter_names=["year", "month", "day", "time"]):
        return {
            form_ele.get("name"): form_ele.get("details", {}).get("values", None)
            for form_ele in self.cds_webapi.get("form")
            if form_ele.get("name") in filter_names
        }
