from pathlib import Path  #Thư viện chuyên nghiệp để làm việc với Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket  # Thư viện để đưa đc dữ liệu từ local lên GCP

from prefect.tasks import task_input_hash
from datetime import timedelta

#test thử
from random import randint




@task(retries=3, cache_key_fn=task_input_hash, cache_expiration= timedelta(days =1)) 
#cache_key_fn=task_input_has --> phần này để chuyển input của task thành hash value. Nếu input của task giống lần trước (ko có gì thay đổi) thì task sẽ đc bỏ qua ko chạy nữa
#cache_expiration= timedelta(days =1) --> để nói rằng sau 1 khoảng thời gian nào đó, kết quả của task được xóa khỏi Prefect cache <Ở đây là sau 1 ngày>
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    # # Test thử xem nếu nó ko chạy sẽ ntn
    # if randint(0,1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df = pd.DataFrame()) -> pd.DataFrame:
    """Fix some DType issue"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    # cái ngoặc vuông ở đây df['tpep_pickup_datetime'] phần này ý là đang chỉ đến cột tpep_pickup_datetime bởi vì df chính là 1 table lớn ta đã lấy đc 
    # và ta đang muốn chỉnh sửa loại dữ liệu ở cột này sang date_time
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"column: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints= True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""     
    path = Path(f"data-{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression= "gzip") 
    # đây là chuyển file về định dạng parqet rồi nén file lại bằng thuật toán gzip
    return path


@task(log_prints=True)
def write_gcs(path: Path) ->None:
    """Upload local to Google Cloud Storage"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"{path}", to_path=path)
    return 


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}" #để thế này bởi vì format của file này ngta public như vậy, với những file khác ta để khác hẳn hoàn toàn
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int =2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)



if __name__ == '__main__':
    months = [1,2,3]
    color = "yellow"
    year = 2021
    etl_parent_flow(months, year, color)








    
