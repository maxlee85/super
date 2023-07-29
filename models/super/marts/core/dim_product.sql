{{ config(materialized='table') }}

with product as (

    select * from {{ ref('stg_product') }}

),

vendor as (

    select * from {{ ref('dim_vendor') }}
)

select
    product_id,
    product_name,
    category_id,
    price,
    cost,
    coalesce(vendor.vendor_id, -1) as vendor_id,
    product.created_date,
    product.created_datetime

from
    product
    left join vendor on product.vendor_id = vendor.vendor_id