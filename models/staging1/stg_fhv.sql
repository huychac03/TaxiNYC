{{ config(materialized='view') }}

with fhv_data as (
    select dispatching_base_num, pickup_datetime, dropOff_datetime, PUlocationID, DOlocationID, SR_Flag, Affiliated_base_number,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging1_1','fhv_table_partition_and_cluster') }}
    where dispatching_base_num is not null
    and PUlocationID is not null
    and DOlocationID is not null
)
select
   dispatching_base_num,
   cast(pickup_datetime as timestamp) as pickup_datetime,
   cast(dropOff_datetime as timestamp) as dropOff_datetime,
   cast(PUlocationID as integer) as pickup_location_id,
   cast(DOlocationID as integer) as dropoff_location_id,
   SR_Flag,
   Affiliated_base_number
from fhv_data
where rn = 1
