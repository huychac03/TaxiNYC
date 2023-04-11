from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os



@task(log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Pull data from web"""
    df = pd.read_csv(dataset_url)
    return df

 
@task(log_prints=True)
def clean(df = pd.DataFrame()) -> pd.DataFrame:
    """Clean Data in df"""
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    print(f"rows: {len(df)}")
    print(f"column : {df.dtypes}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as CSV file"""
    path = Path(f"data-fhv/{dataset_file}.csv")
    df.to_csv(path, compression= "gzip") 
    return path




@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Write to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path = f"{path}", to_path=path, timeout = 1000)
    return


@task(log_prints=True)
def remove_local(path: Path) -> str:
    """Remove in Local"""
    if os.path.exists(path):
        os.remove(path)
    else:
        print("The file does not exist")

    return f"DONE REMOVE TASK"



# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz

@flow(log_prints=True)
def web_to_gcs(month: int, year: int) -> None:
    """The main ETL"""
    
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_cleaned = clean(df)
    path= write_local(df_cleaned, dataset_file)    
    write_gcs(path)
    remove_local(path)



@flow(log_prints=True)
def hw_parameters_flow(
    months: list[int] = [1,2], year: int = 2019
) -> None:
    for month in months:
        web_to_gcs(month, year)




if __name__ == '__main__':
    year = 2019
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    hw_parameters_flow(months, year)
