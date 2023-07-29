{{ config(materialized='table') }}

with orders as (

    select * from {{ ref('fct_order_line') }}

),

first_order as (

    select
        customer_id,
        min(created_date) as first_order_date,
        min(created_datetime) as first_order_datetime,
        min(date_trunc(created_date, month)) as first_order_month

    from
        orders

    group by
        1

)

select * from first_order