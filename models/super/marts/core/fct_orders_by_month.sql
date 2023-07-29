{{ config(materialized='table') }}

with orders as (

    select * from {{ ref('fct_order_line') }}

),

final as (
    
    select
        date_trunc(created_date, month) as month,
        count(distinct order_id) as number_of_orders,
        count(distinct case when is_refunded then order_id end) as number_of_refunds,
        sum(total_price) as gross_merchandise_value,
        {{ find_line_average('total_price', 'order_id') }} as average_order_value,
        {{ find_line_average('quantity', 'order_id') }} as average_basket_size,
        count(distinct customer_id) as active_customers
    
    from
        orders

    {{ dbt_utils.group_by(n=1) }}
    
)

select * from final