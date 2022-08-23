with src_sf_user as(
    select *
    from {{ source('sf', 'user') }}
)

select
    u.name
    ,u.id
from src_sf_user u
