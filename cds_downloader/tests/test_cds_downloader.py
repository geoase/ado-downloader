import os
import pytest
import cdsapi
import math

from cds_downloader import Downloader

from collections import OrderedDict

@pytest.fixture
def era5_downloader():
    return Downloader.from_cds(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "format": "grib",
                "variable": ["2m_temperature", "potential_evaporation"],
                "year": ["1980", "1981"],
                "month": ["01", "02"],
                "day": ["01", "02"],
                "time": ["00:00", "01:00"],
                "area": [50.7, 3.6, 42.9, 17.2]
            }
    )


def test_cds_webapi(era5_downloader):
    assert "form" in era5_downloader.cds_webapi
    assert "selection_limit" in era5_downloader.cds_webapi


def test_org_keys(era5_downloader):
    org_keys = era5_downloader._get_org_keys()
    assert sorted(org_keys) == sorted(["variable", "year", "month", "day", "time"])


def test_request_size(era5_downloader):
    req_size = era5_downloader._get_request_size(["variable", "year", "month", "day", "time"])
    assert req_size == 32


def test_multiprocess_download(era5_downloader):
    era5_downloader.split_keys = ["variable","year"]
    split_filter = era5_downloader._expand_by_keys(era5_downloader.cds_filter, era5_downloader.split_keys)
    lst_processes = era5_downloader._retrieve_files("NO_STORAGE_PATH", split_filter, dry_run=True)
    assert len(lst_processes) == 4


@pytest.mark.parametrize(
    "selection_limit, expected_size",
    [(64, 32), (6, 4), (3, 2)]
)
def test_split_size(era5_downloader, selection_limit, expected_size):
    era5_downloader.cds_webapi["selection_limit"] = selection_limit
    org_keys = era5_downloader._get_org_keys()
    split_keys = era5_downloader._get_split_keys()

    size_keys_request = [k for k in org_keys if k not in split_keys]
    size_request = era5_downloader._get_request_size(size_keys_request)

    assert size_request <= era5_downloader.cds_webapi["selection_limit"]
    assert size_request == expected_size


@pytest.mark.skipif(
    ((os.environ.get('CDSAPI_URL') is None) and
     (os.environ.get('CDSAPI_KEY') is None)),
    reason="requires CDSAPI_KEY and CDSAPI_URL environment variables")
@pytest.mark.parametrize("selection_limit, expected_files", [(64, 1), (6, 8), (3, 16)])
def test_download(era5_downloader, tmp_path, selection_limit, expected_files):
    era5_downloader.cds_webapi["selection_limit"] = selection_limit
    tmpdir = tmp_path / "data"
    tmpdir.mkdir()
    all_processes = era5_downloader.get_data(tmpdir)
    assert len(os.listdir(tmpdir)) == len(all_processes)
    assert len(os.listdir(tmpdir)) == expected_files
