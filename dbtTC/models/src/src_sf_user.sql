with src_sf_user as(
    select *
    from {{ source('salesforce_salesforce', 'user') }}
)

select
    u.id as user_id
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

from
    src_sf_user u
