from pathlib import Path
import pandas as pd
import os
from prefect import flow, task
import pandas_gbq

from prefect_gcp.cloud_storage import GcsBucket


from prefect_gcp import GcpCredentials




@task(log_prints=True)
def extract_from_web(url_data: str) -> pd.DataFrame:
    """Extract data from web"""
    df = pd.read_csv(url_data)
    return df


@task(log_prints=True)
def transform_data(df = pd.DataFrame) -> pd.DataFrame:
    """Transform data"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['VendorID'] = pd.to_numeric(df['VendorID'], errors='coerce').fillna(0).astype(int)
    df['RatecodeID'] = pd.to_numeric(df['RatecodeID'], errors='coerce').fillna(0).astype(int)
    df['passenger_count'] = pd.to_numeric(df['passenger_count'], errors='coerce').fillna(0).astype(int)

    # df['VendorID'] = df['VendorID'].astype(int)
    # df['RatecodeID'] = df['RatecodeID'].astype(int)
    # df['passenger_count'] = df['passenger_count'].astype(int)
    print(f"Rows: {len(df)}")
    print(f"{df.dtypes}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str ) -> Path:
    """Write local as a parquet file"""
    path = Path(f"data-{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression= "gzip") 
    return path


@task(log_prints=True)
def load_to_gcs(path:Path, dataset_file:str) -> None:
    """Load to gcs"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("hw-zoomcamp-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path = path, to_path = f"{dataset_file}.parquet", timeout = 10000)
    return 


@task(log_prints=True)
def remove_local(path: Path) -> str:
    """Remove in Local"""
    if os.path.exists(path):
        os.remove(path)
    else:
        print("The file does not exist")

    return f"DONE REMOVE TASK"


#gs://hw_dtc_data_lake_cryptic-skyline-379306/yellow_tripdata_2020-07.parquet
@flow(log_prints=True)
def etl_load_from_GCS_to_GBQ(mybucket:str, dataset_file:str, table_name:str)->None:
    """GCS to GBQ"""
    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")
    # create a credentials object

    credentials= gcp_credentials_block.get_credentials_from_service_account()
    
    # create a gcs url to the file to be loaded
    gcs_url = f"gs://{mybucket}/{dataset_file}.parquet"
    
    # read the file from gcs
    df = pd.read_parquet(gcs_url)
    
    # load the data into GBQ
    pandas_gbq.to_gbq(
        df,
        destination_table = table_name,
        project_id=credentials.project_id,
        if_exists="append",
        credentials=credentials,
        chunksize=  500_000
    )






@flow(log_prints=True)
def etl_web_to_gcs(color:str, month: int, year: int, dataset_file: str, url_data: str)-> None:
    """First ETL"""
    
    raw_df = extract_from_web(url_data)
    df = transform_data(raw_df)
    path = write_local(df, color, dataset_file)
    load_to_gcs(path, dataset_file)
    remove_local(path)





@flow(log_prints=True)
def full_etl(month: int, year: int, color: str)->None:
    """Full ETL"""
    
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    url_data = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    mybucket = f"hw_dtc_data_lake_cryptic-skyline-379306"
    table_name = f"trips_data_all.{dataset_file}"

    etl_web_to_gcs(color, month, year, dataset_file, url_data)
    etl_load_from_GCS_to_GBQ(mybucket, dataset_file, table_name)
    return


@flow(log_prints=True)
def parent_etl(
    months: list[int] , year: int , color: str 
) -> None:
    for month in months:
        full_etl(month, year, color)


if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1,2]
    parent_etl(months, year, color)

