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
    ,amount
    ,is_won
    ,is_closed
    ,forecast_category

from
    src_sf_opportunity o

where
    is_deleted = 'FALSE'
