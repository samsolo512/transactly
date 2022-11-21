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
    ,o.id as opportunity_id
    ,o.stage_name as stage
--     ,case
--         when o.is_won = 'TRUE' then 1
--         when o.is_won = 'FALSE' then 0
--         end as is_won_flag
    ,o.created_date as created_date_time
    ,cast(o.created_date as date) as created_date

from
    src_sf_opportunity o

where
    is_deleted = 'FALSE'
