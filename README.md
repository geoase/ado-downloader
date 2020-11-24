# ADO Downloader

[![Build Status](https://dev.azure.com/gseyerl/gseyerl/_apis/build/status/geoase.ado-downloader?branchName=master)](https://dev.azure.com/gseyerl/gseyerl/_build/latest?definitionId=1&branchName=master)
[![Documentation Status](https://readthedocs.org/projects/ado-downloader/badge/?version=latest)](https://ado-downloader.readthedocs.io/en/latest/?badge=latest)


This downloader package is part of the EU Interreg Project: Alpine Drought Observation


## Documentation

Package documentation is available at [readthedocs](https://ado-downloader.readthedocs.io/en/latest/).


## Installation

ado-downloader can be installed using the following command:

    python setup.py install

For detailed installation instructions, especially how to install dependencies,
please refer to the
[INSTALL](https://ado-downloader.readthedocs.io/en/latest/install.html) section
of the documentation.


## Climate Data Store Downloader (cds_downloader)

This submodule automates big data downloads from the Copernicus Climate Data
Store. Based on the [cdsapi](https://pypi.org/project/cdsapi/) package, it
enables us to automatically chunk big download requests into parallel processed
sub requests.


### Configuration

The downloader is based on the cdsapi and therefore also uses its authentication
method. In order to use the downloader, one has to create a file with user
credentials [api-how-to](https://cds.climate.copernicus.eu/api-how-to) in the
home directory. Alternatively, define two environment variables 'CDSAPI\_URL'
and 'CDSAPI\_KEY' with the same user credentials. Either way, one needs a
[cds.climate.copernicus.eu](https://cds.climate.copernicus.eu/) user account in
order to use the downloader.


### Download

One approach in order to use the cds\_downloader is to copy the API request from
the cds product page and create the Downloader object with the classmethod
[from_cds](https://ado-downloader.readthedocs.io/en/latest/reference.html#cds_downloader.Downloader.from_cds).
  
![API Request Example from [cds.climate.copernicus.eu](https://cds.climate.copernicus.eu/)](docs/source/images/example_cdsapi.png) <!-- .element width="50%" -->

In order to create a Downloader object, e.g. from the example image above, one
can easily copy the two arguments marked in black.

```python
from cds_downloader import Downloader
  
test_downloader = Downloader.from_cds(
  'reanalysis-era5-single-levels',
  {
    'product_type': 'reanalysis',
    'format': 'grib',
    'variable': '2m_temperature',
    'year': [
      '1979', '1980',
    ],
    'month': [
      '01', '02',
    ],
    'day': [
      '01', '02',
    ],
    'time': [
      '00:00', '01:00',
    ],
  }
)

test_downloader.get_data("storage_path", ["year", "month"])
```
 
To retrieve the data with cdsapi one has to call the method
[get_data](https://ado-downloader.readthedocs.io/en/latest/reference.html#cds_downloader.Downloader.get_data).

The maximum single data request size depends on the copernicus climate data
store and is automatically extracted from their metadata webapi. If split\_keys
are not specified, the method automatically chunks the cds request into multiple
smaller requests and spawns a single process for each of them. Therefore, it
extracts all list-like objects from the cds_filter (e.g. "year", "month", ...) and
splits the data into single requests/files.
