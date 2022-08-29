with src_sf_lead as(
    select *
    from {{ source('sf', 'lead') }}
)

select
    l.first_name
    ,l.last_name
    ,l.name
    ,l.company
    ,l.street
    ,l.city
    ,l.state
    ,l.postal_code
    ,l.country
    ,l.mobile_phone
    ,l.email
    ,l.lead_source
    ,cast(l.created_date as date) as created_date
    ,l.owner_id
    ,l.is_deleted
from src_sf_lead l
