with src_Sugar_contacts as(
    select *
    from {{ source('Sugar', 'contacts') }}
)

select
    *
from src_Sugar_contacts
