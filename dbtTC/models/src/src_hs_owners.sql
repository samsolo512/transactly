-- src_hs_owners

with src_HS_owners as(
    select *
    from {{ source('hs', 'owners') }}
)

select
    ownerid
    ,firstname
    ,lastname
    ,email
from 
    src_HS_owners
