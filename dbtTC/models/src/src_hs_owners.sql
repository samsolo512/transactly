with src_HS_owners as(
    select *
    from {{ source('hs', 'owners') }}
)

select
    ownerid
    ,firstname
    ,lastname
from src_HS_owners
