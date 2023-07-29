{{ config(materialized='table') }}

with customer as (

    select * from {{ ref('stg_customer') }}

),

first_customer_order as (

    select * from {{ ref('int_first_customer_order') }}
),

orders as (

    select * from {{ ref('fct_order_line') }}

)

select
    customer.customer_id,
    name,
    gender,
    email,
    state,
    country,
    customer.created_date,
    customer.created_datetime,
    first_customer_order.first_order_date,
    first_customer_order.first_order_month,
    first_customer_order.first_order_datetime,
    round(sum(total_price), 2) as lifetime_value

from
    customer
    left join first_customer_order on customer.customer_id = first_customer_order.customer_id
    left join orders on customer.customer_id = orders.customer_id

{{ dbt_utils.group_by(n=11) }}