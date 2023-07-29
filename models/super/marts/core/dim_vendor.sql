{{ config(materialized='table') }}

with vendor as (

    select * from {{ ref('stg_vendor') }}

)

select
    vendor_id,
    vendor_name,
    created_date,
    created_datetime

from
    vendor