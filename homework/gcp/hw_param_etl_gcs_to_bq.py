from pathlib import Path
import pandas as pd 
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) ->Path:
    """Download trip data from GCS"""
    gcs_path = f"data-{color}\{color}_tripdata_{year}-{month:02}.parquet"
    # đầy là path của file chứa dữ liệu trên GCS
    gcs_block = GcsBucket.load("zoomcamp-gcs") # Truy cập vào GCS nhờ block
    gcs_block.get_directory(from_path = gcs_path, local_path =f"./data-yellow/")
    # get_directory là 1 method dùng để tải dữ liệu từ nơi này sang nơi khác
    return Path(f"data-yellow/{gcs_path}")



@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Cleanning the data"""
    df = pd.read_parquet(path)
    print(f"Number of rows: {len(df)} " )
    print(f"Pre: Missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"Pre: Missing passenger count: {df['passenger_count'].isna().sum()}")
    return df



@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")
    
    df.to_gbq(
        destination_table="HW_trips_data_all.yellow_data_trips",
        project_id= "cryptic-skyline-379306",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=  500_000,
        if_exists= "append"
    )


@flow(log_prints=True)
def etl_load_1_gcs_to_bq(month:int, year: int, color: str)->None:
    """ ETL to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


@flow(log_prints=True)
def etl_load_from_gcs_to_gbq(months: list[int] = [1,2], year: int = 2020, color: str ="yellow" )->None:
    for month in months:
        etl_load_1_gcs_to_bq(month, year, color)





if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [1,2]
    etl_load_from_gcs_to_gbq(months, year, color)