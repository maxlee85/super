with orders as (

    select *
    from {{ ref('fct_orders') }}

)

select order_id
from orders
where customer_id is null