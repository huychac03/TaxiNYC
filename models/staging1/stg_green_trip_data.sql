{{ config(materialized='view') }}

with data_green as (

    select *,
        row_number() over(partition by VendorID, lpep_pickup_datetime) as rn
    from {{ source('staging1_2', 'green_data') }}
    where VendorID is not null   
    
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'lpep_pickup_datetime']) }} as trip_id,  -- dùng để sinh ra khóa đặc biệt cho mỗi row
    cast(VendorID as integer) as vendor_id,
    cast(RatecodeID as integer) as ratecode_id,
    cast(PULocationID as integer) as  pickup_location_id,
    cast(DOLocationID as integer) as dropoff_location_id,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge
from data_green
where rn = 1