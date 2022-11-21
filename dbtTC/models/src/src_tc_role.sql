with src_tc_role as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'role') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    r.id as role_id
    ,r.role
    ,r.name as role_name
from
    src_tc_role r
where
    _fivetran_deleted = 'FALSE'
