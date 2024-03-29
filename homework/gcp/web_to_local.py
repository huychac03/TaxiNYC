from pathlib import Path
import pandas as pd
from prefect import flow, task
import os



@task(log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Pull data from web"""
    df = pd.read_csv(dataset_url)
    return df

 
@task(log_prints=True)
def clean(df = pd.DataFrame()) -> pd.DataFrame:
    """Clean Data in df"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['VendorID'] = pd.to_numeric(df['VendorID'], errors='coerce').fillna(0).astype(int)
    df['RatecodeID'] = pd.to_numeric(df['RatecodeID'], errors='coerce').fillna(0).astype(int)
    df['passenger_count'] = pd.to_numeric(df['passenger_count'], errors='coerce').fillna(0).astype(int)
    df['payment_type'] = pd.to_numeric(df['payment_type'], errors='coerce').fillna(0).astype(int)
    #df['trip_type'] = pd.to_numeric(df['trip_type'], errors='coerce').fillna(0).astype(int)

    print(f"rows: {len(df)}")
    print(f"column : {df.dtypes}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data-{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression= "gzip") 
    return path






@flow(log_prints=True)
def web_to_gcs(month: int, year: int, color: str) -> None:
    """The main ETL"""
    
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_cleaned = clean(df)
    path= write_local(df_cleaned, color, dataset_file)    




@flow(log_prints=True)
def hw_parameters_flow(
    months: list[int] = [1,2], year: int = 2019, color: str ="yellow"
) -> None:
    for month in months:
        web_to_gcs(month, year, color)




if __name__ == '__main__':
    color = "yellow"
    year = 2020
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    hw_parameters_flow(months, year, color)
