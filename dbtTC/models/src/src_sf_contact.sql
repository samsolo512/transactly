with src_sf_contact as(
    select *
    from {{ source('salesforce_salesforce', 'contact') }}
)

select
    c.agent_c
    ,c.agent_brokerage_c
    ,c.id as contact_id
    ,{{ field_clean('c.mailing_street') }} as street
    ,c.mailing_city as city
    ,c.mailing_state as state
    ,c.mailing_postal_code as zip
    ,c.email
    ,c.account_id
    ,c.last_name
    ,{{ field_clean('c.first_name') }} as first_name
    ,c.name as full_name
    ,c.mobile_phone
    ,c.mobile_phone as phone
    ,c.owner_id
    ,c.created_date as created_date_time
    ,cast(c.created_date as date) as created_date
    ,converted_lead_c
    ,converted_lead_c as lead_id
    ,{{ field_clean('c.regulated_electricity_c') }} as electricity
    ,{{ field_clean('c.regulated_sewer_c') }} as sewer
    ,{{ field_clean('c.regulated_trash_c') }} as trash
    ,{{ field_clean('c.regulated_water_c') }} as water
    ,{{ field_clean('c.regulated_gas_c') }} as gas
    ,{{ field_clean('c.telecom_c') }} as internet
    ,attribution_c as attribution

from
    src_sf_contact c

where
    is_deleted = 'FALSE'
