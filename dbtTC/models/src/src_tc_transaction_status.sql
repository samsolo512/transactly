with src_tc_transaction_status as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'transaction_status') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    t.id as transaction_status_id
    ,t.name as status
from src_tc_transaction_status t
where _fivetran_deleted = 'FALSE'
