with src_sf_opportunity as(
    select *
    from {{ source('sf', 'opportunity') }}
)

select
    o.contact_id
    ,o.account_id
    ,cast(o.close_date as date) as close_date
    ,o.name as opportunity_name
    ,o.owner_id
    ,o.id as opportunity_id
    ,o.stage_name as stage
    ,o.is_won as is_won_flag
from src_sf_opportunity o
