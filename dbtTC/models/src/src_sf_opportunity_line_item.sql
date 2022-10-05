with src_sf_opportunity_line_item as(
    select *
    from {{ source('sf', 'opportunity_line_item') }}
)

select
    o.product_revenue_c as revenue
    ,o.product_2_id as product_id
    ,o.opportunity_id
    ,o.name as opportunity_line_item_name
from src_sf_opportunity_line_item o
where
    is_deleted = 'FALSE'
