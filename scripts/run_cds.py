import click
import logging

from cds_downloader import Downloader

def default_none(ctx, param, value):
    if len(value) == 0:
        return None
    else:
        return value

@click.command()
@click.option('--config', '-c', required=True, type=click.Path(exists=True), help='JSON configuration file')
@click.option('--path', '-p', 'storage_path', required=True, type=click.Path(), help="""Target storage path""")
@click.option('--mode', '-m', default='download', type=click.Choice(['download', 'update', 'daily'], case_sensitive=True),
              help="""The operational mode 'update' is experimental. It is recommended to provide
              the exact same set of split-keys from the already existing data collection.""")
@click.option('--split-keys', "-sk", multiple=True, callback=default_none,
              help="""By setting multiple values of split_key from cds_filter keys,
              one can manually control the splitting (e.g. -sp year -sp month -sp day)""")
@click.option('--start-from-files', '-sff', 'start_from_files', type=bool, default=False,
              help="""Only available in update mode. Start data update from last file (experimental)""")
@click.option('--date-latency', '-dl', 'date_latency', type=str, default=False,
              help="""Only available in update mode. Specify start date latency from now backwards, e.g.
              '5D' or '2D 8h 5m 2s' (experimental)""")
@click.option('--log-path', '-lp', 'log_path', type=click.Path(), help="""Path to logging file""")
@click.option('--log-level', '-ll', 'log_level', default="WARNING",
              type=click.Choice(["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], case_sensitive=True),
              help="""Logging Level""")

def start(config, storage_path, mode, split_keys, start_from_files, date_latency, log_path, log_level):
    """CDS Downloader command line interface"""

    if log_path != None:
        logging.basicConfig(filename=log_path, format='%(asctime)s %(message)s', level=log_level)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

    # Convert tuple to list if split keys are specified
    if isinstance(split_keys, tuple):
        split_keys = list(split_keys)

    # Create Downloader object
    cds_downloader = Downloader.from_json(config)

    if mode == "download":
        cds_downloader.get_data(storage_path, split_keys)
    elif mode == "update":
        cds_downloader.update_data(storage_path, split_keys, start_from_files=start_from_files, date_latency=date_latency)
    elif mode == "daily":
        cds_downloader.get_latest_daily_data(storage_path, date_latency=date_latency)



if __name__ == '__main__':
    start()
