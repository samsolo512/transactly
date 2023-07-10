-- src_tc_user_vendor

with src_tc_user_vendor as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'user_vendor') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    a.id as user_id
    ,to_date(a.updated) as updated
from 
    src_tc_user_vendor a
where 
    _fivetran_deleted = 'FALSE'
