with src_sf_product_2 as(
    select *
    from {{ source('sf', 'product_2') }}
)

select
    p.name as product_name
    ,p.id as product_id
    ,family as product_family
    ,vendor_c as vendor_id
    ,vendor_code_c as vendor_code
from src_sf_product_2 p
where
    is_deleted = 'FALSE'