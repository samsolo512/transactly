with src_tc_transactly_vendor as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'transactly_vendor') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    id as vendor_id
    ,vendor_type_id
    ,internal_name
from src_tc_transactly_vendor
where _fivetran_deleted = 'FALSE'
