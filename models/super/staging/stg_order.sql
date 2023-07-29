with `order` as (

    select * from {{ source('super', 'order') }}

)

select
    id as order_id,
    customer_id,
    currency,
    total_price,
    cast(created_at as date) as created_date,
    created_at as created_datetime,
    cast(refunded_at as date) as refunded_date,
    refunded_at as refunded_datetime,
    case when refunded_at is not null then true else false end as is_refunded,
    case when refunded_at is not null then -1.00*total_price else 0 end as refunded_amount
    
from 
    `order`