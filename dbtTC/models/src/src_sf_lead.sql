with src_sf_lead as(
    select *
    from {{ source('sf', 'lead') }}
)

select
    l.id as lead_id
    ,l.first_name
    ,l.last_name
    ,l.name
    ,l.company
    ,{{ field_clean('l.street') }} as street
    ,l.city
    ,l.state
    ,l.postal_code as zip
    ,l.country
    ,l.mobile_phone as phone
    ,l.email
    ,l.lead_source
    ,l.created_date as created_date_time
    ,cast(l.created_date as date) as created_date
    ,l.owner_id
    ,l.is_deleted
from src_sf_lead l
where
    is_deleted = 'FALSE'
