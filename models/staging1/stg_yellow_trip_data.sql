{{ config(materialized='view') }}

select * from {{ ref('stg_yellow_data') }}