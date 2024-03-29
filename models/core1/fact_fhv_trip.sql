{{ config(materialized='table') }}

with green_data as (
    select *,
        'Green' as service_type
    from {{ ref('stg_green_trip_data') }}
),

yellow_data as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_trip_data') }}
),

trips_unioned as (
    select * from green_data
    union all 
    select * from yellow_data
),

fhv_data as (
    select * 
    from {{ ref('stg_fhv') }}
)

select
    trips_unioned.trip_id, 
    trips_unioned.vendor_id, 
    trips_unioned.service_type,
    trips_unioned.ratecode_id, 
    trips_unioned.pickup_location_id, 
    --pickup_zone.Borough as pickup_borough, 
    --pickup_zone.Zone as pickup_zone, 
    trips_unioned.dropoff_location_id,
    --dropoff_zone.Borough as dropoff_borough, 
    --dropoff_zone.Zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description, 
    trips_unioned.congestion_surcharge
from trips_unioned
inner join fhv_data 
on trips_unioned.pickup_location_id = fhv_data.pickup_location_id
--and trips_unioned.dropoff_location_id = fhv_data.dropoff_location_id
and trips_unioned.pickup_datetime = fhv_data.pickup_datetime
--and trips_unioned.dropoff_datetime = fhv_data.dropOff_datetime