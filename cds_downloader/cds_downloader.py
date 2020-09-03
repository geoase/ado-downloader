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

        # User Credentials from environment variables
        # 'CDSAPI_URL' and 'CDSAPI_KEY'
        self.cdsapi_client = cdsapi.Client()

        # Create storage path
        Path(storage_path).mkdir(parents=True, exist_ok=True)

        # If necessary, find keys for download chunking
        if split_keys is None:
            split_keys = self._get_split_keys()

        split_filter = self._expand_by_keys(self.cds_filter, split_keys)
        all_processes = []
        for cds_filter in split_filter:
            file_path = '-'.join([cds_filter.get(k) for k in split_keys] or ["all"]) + \
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
    Update data collecation at end and attach to last file or create new file if necessary
    Under development. For now only temporal update at end of time series
    """
    # def __init__(self, **kwargs):
    #     super().__init__(**kwargs)

    #     self.cds_time_filter_api, self.cds_time_dim_api = self._get_time_filter_and_dim_from_webapi()

    def update_data(self, split_keys, path_string, **kwargs):

        # TODO:
        # - Problem if split_keys includes non temporal attributes

        path_storage = Path(path_string)

        if path_storage.is_dir():
            lst_existing_files = [f for f in path_storage.glob("*." + self.cds_filter.get("format", "grib"))]
            lst_existing_files = sorted(lst_existing_files)
        else:
            raise("No valid path")


        # Load data and extract time dimension
        xds_existing_data = xarray.open_mfdataset(lst_existing_files[-1].as_posix(), **kwargs)

        try:
            dim_time_storage = xds_existing_data.valid_time.to_series().values
            freq_from_storage = xds_existing_data.valid_time.to_series().diff()
        except AttributeError:
            raise("No dimension 'valid_time' found in stored dataset")

        if freq_from_storage.nunique() == 1:
            freq_from_storage = pandas.DateOffset(seconds=freq_from_storage[-1].seconds)
        else:
            raise("No continuous time frequency in storage data")

        # Define new time dimension from storage info
        dim_time_since_last = pandas.date_range(
            start=dim_time_storage[-1],
            end=datetime.datetime.now(),
            freq=freq_from_storage,
            closed="right"
        )

        # Adapt filter
        year = [str(ele) for ele in dim_time_since_last.year.unique().sort_values()]
        month = [str(ele) for ele in dim_time_since_last.month.unique().sort_values()]
        day = [str(ele) for ele in dim_time_since_last.day.unique().sort_values()]
        time = [ele.isoformat() for ele in numpy.unique(dim_time_since_last.time)]

        self.cds_filter.update(
        {"year": year,
         "month": month,
         "day": day,
         "time": time
        })

        # Download new data in temporary folder
        temporary_path = tempfile.mkdtemp()
        super(ClimateDataStoreUpdater, self).get_data(temporary_path, split_keys)

        lst_new_files = [f for f in Path(temporary_path).iterdir()]

        # assert lst_new_files[0].rsplit("/",1)[-1] == path.rsplit("/",1)[-1], "Merge Problem, check your split_keys"

        # Add data to existing file or create new file if necessary
        # Merge first file
        import ipdb; ipdb.set_trace()
        path_merge_file = os.path.join(temporary_path, "tmp_merge.grib")
        subprocess.call([
            "grib_copy",
            lst_existing_files[-1],
            lst_new_files[0],
            path_merge_file
        ])

        shutil.move(path_merge_file, lst_new_files[0].as_posix())

        # Move rest of files if
        for f in lst_new_files:
            # Move and overwrite merged file
            shutil.move(f, path_storage.joinpath(f.name))



    def _get_time_filter_and_dim_from_webapi(self):
        date_now = datetime.datetime.utcnow()

        # Extract temporal filter from webapi
        filter_date_webapi = self._full_time_filter_from_webapi()

        # Assume temporal frequency from filter
        cds_extracted_freq = self._freq_from_time_filter(filter_date_webapi)

        # Acquire start and end date
        start_date = {}
        end_date = {}
        for k,v in filter_date_webapi.items():
            if k == "time":
                start_date["hour"],start_date["minute"] = (map(int, v[0].split(":")))
                end_date["hour"],start_date["minute"] = (map(int, v[-1].split(":")))
            else:
                start_date[k] = int(v[0])
                end_date[k] = int(v[-1])

        # Define time dimension from webapi
        dim_time_webapi = pandas.date_range(
            start=datetime.datetime(**start_date),
            end=datetime.datetime(**end_date),
            freq=cds_extracted_freq
        )

        # Cut time dimension if last date is bigger than today
        if dim_time_webapi[-1] > date_now:
            dim_time_webapi = dim_time_webapi[:dim_time_webapi.get_loc(date_now, "ffill")]

        return filter_date_webapi, dim_time_webapi


    def _full_time_filter_from_webapi(self, filter_names=["year", "month", "day", "time"]):
        return {
            form_ele.get("name"): form_ele.get("details", {}).get("values",None)
            for form_ele in self.cds_webapi.get("form")
            if form_ele.get("name") in filter_names
        }

    def _freq_from_time_filter(self, filter_date_webapi):
        # Assume temporal frequency
        if "time" in filter_date_webapi:
            avail_freq="{}H".format(24/len(filter_date_webapi.get("time")))
        elif "day" in filter_date_webapi:
            avail_freq="1D"
        elif "month" in filter_date_webapi:
            avail_freq="1M"
        elif "year" in filter_date_webapi:
            avail_freq="1Y"

        return avail_freq
