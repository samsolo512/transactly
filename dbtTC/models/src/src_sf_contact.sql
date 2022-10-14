with src_sf_contact as(
    select *
    from {{ source('sf', 'contact') }}
)

select
    c.agent_c
    ,c.agent_brokerage_c
    ,c.id as contact_id
    ,{{ field_clean('c.mailing_street') }} as street
    ,c.mailing_state as state
    ,c.mailing_postal_code as zip
    ,c.email
    ,c.account_id
    ,c.last_name
    ,c.first_name
    ,c.name as full_name
    ,c.mobile_phone as phone
    ,c.owner_id
    ,c.created_date as created_date_time
    ,cast(c.created_date as date) as created_date
    ,converted_lead_c
    ,converted_lead_c as lead_id
    ,regulated_electricity_c as electricity
    ,regulated_sewer_c as sewer
    ,regulated_trash_c as trash
    ,regulated_water_c as water
    ,regulated_gas_c as gas

from
    src_sf_contact c

where
    is_deleted = 'FALSE'
