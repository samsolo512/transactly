with src_Sugar_revenueLineItems as(
    select *
    from {{ source('Sugar', 'revenuelineitems') }}
)

select
    *
from src_Sugar_revenueLineItems