
import argparse

import os

from time import time
from datetime import timedelta

import pandas as pd

from sqlalchemy import create_engine

import pyarrow.parquet as pq
import pyarrow.csv as csv


from prefect import task, flow
from prefect.tasks import task_input_hash

#gọi thư viện này ra để ta có thể dùng Block trong Prefect cho tiện đỡ phải nhập lại nhiều lần
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    file_name = 'yellow_tripdata_2021-01.parquet'
    # download the CSV
    #nhưng ở đây ko có file CSV nên phải download file barquet rồi chuyển về CSV
    if (os.path.isfile(file_name) == False):
        os.system(f"wget {url} -O {file_name}")

    if (os.path.isfile('yellow_tripdata_2021-01.csv') == False):
        table = pq.read_table('yellow_tripdata_2021-01.parquet')
        csv.write_csv(table, 'yellow_tripdata_2021-01.csv')
    #đã chuyển về CSV xong hehee

    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()


    df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)


    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count : {df['passenger_count'].isin([0]).sum()} ")
    print(f"Start Transforming Data")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count : {df['passenger_count'].isin([0]).sum()} ")
    return df


@task(log_prints=True, retries=3)
#@task()
def ingest_data(table_name, df):
    
    connection_block = SqlAlchemyConnector.load("first-sqlaichemy-connector")

    with connection_block.get_connection(begin = False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists = 'replace')
        df.to_sql(name=table_name, con=engine, if_exists = 'append')


# Trong flow có thể có các subflows
@flow(name ="SubFlow", log_prints=True)
def log_subflows(table_name: str):
    print("Logging subflows for {table_name} ")

 
@flow(name="Ingest")
def mainflow(table_name: str):
    #a_table_name =table_name 
    download_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"


    log_subflows(table_name)    
    raw_data = extract_data(download_url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)



 
if __name__ == '__main__':
    mainflow("new_yellow_taxi_trips")

