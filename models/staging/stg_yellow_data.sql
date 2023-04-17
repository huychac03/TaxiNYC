{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by VendorID, tpep_pickup_datetime) as rn
  from {{ source('staging','yellow_data') }}
  where VendorID is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as trip_id,
    cast(VendorID as integer) as vendor_id,
    cast(RatecodeID as integer) as ratecode_id,
    cast(PULocationID as integer) as  pickup_location_id,
    cast(DOLocationID as integer) as dropoff_location_id,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    cast(congestion_surcharge as numeric) as congestion_surcharge
from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default = true) %}
    --limit 100
{% endif %}

{# This is a dbt macro code that uses a conditional statement to check the value of the is_test_run variable. 
    If the variable is not defined, it will use a default value of true.

If the value of is_test_run is true, then the macro will add a LIMIT 100 clause to the SQL query. 
If the value of is_test_run is false or not defined, then the LIMIT clause will not be added to the query.

This macro can be useful when you want to limit the number 
of rows returned by a query during testing, but not during production. #}
