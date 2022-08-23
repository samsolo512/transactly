with src_Sugar_opportunities as(
    select *
    from {{ source('Sugar', 'opportunities') }}
)

select
    *
from src_Sugar_opportunities
