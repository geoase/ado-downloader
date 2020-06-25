import os
import pytest

from cds_downloader import ClimateDataStoreDownloader

@pytest.fixture
def era5_downloader():
    return ClimateDataStoreDownloader.from_cds(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "format": "grib",
                "variable": [ "2m_temperature","potential_evaporation"],
                "year": ["1979", "1980"],
                "month": ["01", "02"],
                "day": ["01", "02"],
                "time": ["00:00", "01:00"],
                "area": [50.7, 3.6, 42.9, 17.2]
            }
    )


def test_request_size(era5_downloader):
    req_size = era5_downloader._get_request_size(
        ["year", "month", "day", "time", "variable"])
    assert req_size == 32

def test_split_key(era5_downloader):
    era5_downloader.cds_webapi["selection_limit"] = 4
    split_keys = era5_downloader._get_split_keys()
    assert split_keys == ['year', 'month', 'day', 'time']
    # assert split_keys == []
