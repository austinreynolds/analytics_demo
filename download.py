import duckdb
import time
import json
import os
import pandas as pd
from pathlib import Path
from zipfile import ZipFile
from urllib.request import urlopen
from src.sharadar_data_dict import sharadar_raw_dtypes


def fetch_sharadar_table(tablename, table_url, download_path, n_retries=2):
    """
    Download and extract a Sharadar table (CSV) from Nasdaq data link.

    Args:
        tablename (str): Name of the table to download.
        download_path (Path or str): Directory to store downloaded data.
        table_url (str): Completed endpoint URL for the table, including API key argument.

    Raises:
        Exception: If there are issues during download or extraction.

    Returns:
        Filepath of CSV file (Path).
    """  

    download_path = Path(download_path)
    os.makedirs(download_path, exist_ok=True)
    zipfile_path = download_path.joinpath(tablename + '.zip')
    tmpdir_path = download_path.joinpath('tmp')
    os.makedirs(tmpdir_path, exist_ok=True)
   
    # Retrieve download link
    valid = ['fresh','regenerating']
    invalid = ['generating']
    status = ''   
    n_tries = 0
    while (status not in valid) and (n_tries < n_retries + 1):
        n_tries += 1
        response_dict = json.loads(urlopen(table_url).read())
        # last_refreshed_time = response_dict['datatable_bulk_download']['datatable']['last_refreshed_time']
        status = response_dict['datatable_bulk_download']['file']['status']
        remote_file_link = response_dict['datatable_bulk_download']['file']['link']
        if status not in valid:
            time.sleep(30)

    # Download
    zipString = urlopen(remote_file_link).read()
    f = open(zipfile_path, 'wb')
    f.write(zipString)
    f.close()
    
    # Extract from zipfile
    with ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(tmpdir_path)

    # Clean up
    csv_orig_filepath = tmpdir_path.joinpath(os.listdir(tmpdir_path)[0])
    csv_new_filepath = download_path.joinpath(tablename + '.csv')
    os.rename(csv_orig_filepath, csv_new_filepath)
    os.remove(zipfile_path)
    os.rmdir(tmpdir_path)

    return csv_new_filepath


if __name__ == "__main__":
    sharadar_base_url = "https://data.nasdaq.com/api/v3/datatables/SHARADAR/{tblname}.json?qopts.export=true&api_key={apikey}"
    sharadar_tablenames = ['actions', 'daily', 'indicators', 'metrics', 'sep', 'sf1', 'sf2', 'sf3', 'sfp', 'tickers']

    data_dir = os.getenv('DATA_HOME')
    if not data_dir:
        raise ValueError("DATA_HOME environment variable not found!")
    else:
        data_dir = Path(data_dir).joinpath("analytics_demo")
        duckdb_path = data_dir.joinpath("sharadar.duckdb")

    api_key = os.getenv('NASDAQ_DATA_API_KEY')
    if not api_key:
        raise ValueError("NASDAQ_DATA_API_KEY environment variable not found!")

    downloaded_tablenames = []
    downloaded_table_paths = []
    for tn in sharadar_tablenames:
        # Download CSV
        download_url = sharadar_base_url.format(tblname=tn, apikey=api_key)
        csv_path = fetch_sharadar_table(tn, download_url, data_dir, n_retries=1)

        # Convert to parquet, remove CSV
        parquet_path = csv_path.with_suffix('.parquet')
        dtypes = sharadar_raw_dtypes[tn]
        date_fields = [x for x in dtypes if dtypes[x] == "date"]
        nondate_fields = {x: dtypes[x] for x in dtypes if dtypes[x] != "date"}
        df_raw = pd.read_csv(csv_path, dtype=nondate_fields, parse_dates=date_fields)
        df_raw.to_parquet(parquet_path)
        os.remove(csv_path)

        downloaded_tablenames.append(tn)
        downloaded_table_paths.append(parquet_path)
        # print(tn, "table downloaded and converted.")
    
    # Load tables into duckdb file
    with duckdb.connect(database=str(duckdb_path)) as con:
        df_sf1_columns_metadata = pd.read_csv('src/sf1_columns_metadata.csv')
        con.execute('create or replace table sf1_columns_metadata as select * from df_sf1_columns_metadata')

        insert_table_str = """create or replace table {tblnm} as select * from read_parquet('{pq_path}')"""
        for tn, tp in zip(downloaded_tablenames, downloaded_table_paths):
            con.execute(insert_table_str.format(tblnm=tn, pq_path=str(tp)))
            # print(tn, 'loaded into duckdb')

    con.close()
