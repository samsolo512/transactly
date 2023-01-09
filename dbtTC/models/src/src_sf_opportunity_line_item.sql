-- src_sf_opportunity_line_item

with src_sf_opportunity_line_item as(
    select *
    from {{ source('salesforce_salesforce', 'opportunity_line_item') }}
)

select
    o.product_revenue_c as revenue
    ,o.product_2_id as product_id
    ,o.opportunity_id
    ,o.name as opportunity_line_item_name
    ,case 
        when o.paid_c = 'TRUE' then 1
        when o.paid_c = 'FALSE' then 0
        else null
        end as paid_flag

from 
    src_sf_opportunity_line_item o

where
    is_deleted = 'FALSE'
