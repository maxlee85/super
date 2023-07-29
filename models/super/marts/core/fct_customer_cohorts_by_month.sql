{{ config(materialized='table') }}

with calendar as (

    select * from {{ ref('calendar') }}

),

orders as (

    select * from {{ ref('fct_order_line') }}
),

first_customer_order as (

    select * from {{ ref('int_first_customer_order') }}

),

customer_months as (

    select
        first_customer_order.customer_id,
        calendar.date_month,
        first_customer_order.first_order_month
    from
        calendar 
        join first_customer_order on first_customer_order.first_order_month <= calendar.date_month

),

orders_customer_months as (

    select
        customer_months.customer_id,
        customer_months.date_month,
        customer_months.first_order_month,
        sum(orders.net_price) as net_price

    from
        customer_months
        left join orders 
            on customer_months.customer_id = orders.customer_id
            and customer_months.date_month = date_trunc(orders.created_date, month)

    {{ dbt_utils.group_by(n=3) }}

),

month_cohorts as (

    select distinct
        date_month as month,
        dense_rank() over (order by date_month) as cohort_number

    from
        orders_customer_months

    where
        date_month = first_order_month

),

calculations as (

    {% set partition_string = 'partition by customer_id order by date_month' %}

    select
        {{ dbt_utils.generate_surrogate_key(['date_month', 'customer_id']) }} as customer_cohort_id,
        customer_id,
        date_month,
        case when net_price is not null then true else false end as is_active,
        first_order_month,
        net_price,
        sum(net_price) over ( {{ partition_string }} ) as lifetime_value,
        month_cohorts.cohort_number,
        row_number() over ( {{ partition_string }} ) as cohort_month_number
        
    from
        orders_customer_months
        left join month_cohorts on orders_customer_months.first_order_month = month_cohorts.month
)

select * from calculations