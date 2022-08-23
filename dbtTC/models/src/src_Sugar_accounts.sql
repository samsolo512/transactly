with src_Sugar_accounts as(
    select *
    from {{ source('Sugar', 'accounts') }}
)

select
    *
from src_Sugar_accounts
