with src_tc_transaction_role as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'transaction_role') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    t.id as role_id
    ,t.name as role_name
    ,t.active as active_flag
from src_tc_transaction_role t
where _fivetran_deleted = 'FALSE'
