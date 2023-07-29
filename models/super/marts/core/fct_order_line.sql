{{ config(materialized='table') }}

with `order` as (

    select * from {{ ref('stg_order') }}

),

order_line as (

    select * from {{ ref('stg_order_line') }}

),

product as (

    select * from {{ ref('dim_product') }}

),

customer as (

    select * from {{ ref('stg_customer') }}
),

final as (

    select
        order_line.order_line_id,
        order_line.order_id,
        coalesce(customer.customer_id, -1) as customer_id,
        coalesce(order_line.product_id, -1) as product_id,
        coalesce(product.vendor_id, -1) as vendor_id,
        order_line.quantity,
        order_line.total_price,
        case 
            when `order`.is_refunded then order_line.total_price - order_line.total_price 
            else order_line.total_price 
        end as net_price,
        `order`.created_date,
        `order`.created_datetime,
        `order`.is_refunded,
        `order`.refunded_date,
        `order`.refunded_datetime
        
    from
        order_line
        join `order` on order_line.order_id = `order`.order_id
        left join product on order_line.product_id = product.product_id
        left join customer on `order`.customer_id = customer.customer_id

)

select * from final