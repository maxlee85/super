with order_line as (

    select * from {{ source('super', 'order_line') }}

)

select
    id as order_line_id,
    order_id,
    product_id,
    quantity,
    total_price
    
from
    order_line