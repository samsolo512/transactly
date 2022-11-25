with src_tc_office_user as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'office_user') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    o.id as office_user_id
    ,o.user_id
    ,o.office_id
    ,o.created
from
    src_tc_office_user o
where
    _fivetran_deleted = 'FALSE'
