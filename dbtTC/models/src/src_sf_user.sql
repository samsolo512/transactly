with src_sf_user as(
    select *
    from {{ source('sf', 'user') }}
)

select
    u.id
    ,u.first_name
    ,u.last_name
    ,u.name
    ,u.title
    ,u.street
    ,u.city
    ,u.postal_code
    ,u.country
    ,u.email
    ,u.phone
    ,u.mobile_phone
    ,u.is_active
from src_sf_user u
