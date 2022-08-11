with src_sf_product_2 as(
    select *
    from fivetran.salesforce.product_2
)

select
    p.name as product_name
    ,p.id as product_id
    ,family as product_family
from src_sf_product_2 p
