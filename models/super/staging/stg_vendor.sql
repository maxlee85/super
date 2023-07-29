with vendor as (
    
    select * from {{ source('super', 'vendor')}}

)

select
    id as vendor_id,
    title as vendor_name,
    cast(created_at as date) as created_date,
    created_at as created_datetime
    
from
    vendor