-- src_sf_lead

with src_sf_lead as(
    select *
    from {{ source('salesforce_salesforce', 'lead') }}
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
    ,l.mobile_phone
    ,l.phone
    ,l.email
    ,l.lead_source
    ,l.created_date as created_date_time
    ,cast(l.created_date as date) as created_date
    ,l.owner_id
    ,l.is_deleted
    ,{{ field_clean('l.agent_c') }} as agent_name
    ,l.agent_email_c as agent_email
    ,regulated_electricity_c as electricity
    ,regulated_sewer_c as sewer
    ,regulated_trash_c as trash
    ,regulated_water_c as water
    ,regulated_gas_c as gas
    ,converted_date
    ,is_converted
    ,status

from 
    src_sf_lead l
    
where
    is_deleted = 'FALSE'
