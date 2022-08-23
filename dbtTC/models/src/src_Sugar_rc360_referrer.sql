with src_Sugar_RC360_referrer as(
    select *
    from {{ source('Sugar', 'rc360_referrer') }}
)

select
    *
from src_Sugar_RC360_referrer