with product as (

    select * from {{ source('super', 'product') }}

)

select
    id as product_id,
    title as product_name,
    category as category_id,
    price,
    cost,
    vendor as vendor_id,
    cast(created_at as date) as created_date,
    created_at as created_datetime
    
from
    product