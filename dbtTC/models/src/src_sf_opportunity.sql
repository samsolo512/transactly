-- src_sf_opportunity

with src_sf_opportunity as(
    select *
    from {{ source('salesforce_salesforce', 'opportunity') }}
)

select
    o.contact_id
    ,o.account_id
    ,o.close_date
    ,o.name as opportunity_name
    ,o.owner_id
    ,trim(o.id) as opportunity_id
    ,o.stage_name as stage
    ,o.created_date as created_date_time
    ,to_date(o.created_date) as created_date
    ,to_date(o.last_stage_change_date) as last_stage_change_date
    ,o.amount
    ,o.is_won
    ,o.is_closed
    ,o.forecast_category
    ,case 
        when o.paid_c = 'TRUE' then 1
        when o.paid_c = 'FALSE' then 0
        else null
        end as paid_flag
    ,lease_begins_c as lease_start_date
    ,service_date_begins_c as service_start_date

from
    src_sf_opportunity o

where
    is_deleted = 'FALSE'
