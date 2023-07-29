{{ config(materialized='table') }}

{%- set cohort_month_numbers = [1,2,3,4,5,6,7,8,9,10,11,12] -%}

with customer_cohorts as (

    select * from {{ ref('fct_customer_cohorts_by_month') }}

),

pivoted as (

    select
        cohort_number,
        customer_id,
        first_order_month,
        {% for cohort_month_number in cohort_month_numbers -%}
        case when is_active and cohort_month_number = {{ cohort_month_number }} then 1 else 0 end as is_cohort_month_{{ cohort_month_number }}_customer
            {%- if not loop.last -%}
            ,
            {%- endif %}
        {% endfor -%}

    from
        customer_cohorts

    group by
        cohort_number,
        first_order_month,
        customer_id,
        is_active,
        cohort_month_number

)

select * from pivoted