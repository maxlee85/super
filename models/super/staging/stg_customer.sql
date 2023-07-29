with customer as (

    select * from {{ source('super', 'customer') }}

)

select
    id as customer_id,
    name,
    gender,
    email,
    state,
    country,
    cast(created_at as date) as created_date,
    created_at as created_datetime
    
from
    customer  

