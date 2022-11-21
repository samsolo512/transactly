with src_tc_user_role as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'user_role') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    ur.user_id
    ,ur.role_id
from src_tc_user_role ur
where _fivetran_deleted = 'FALSE'
