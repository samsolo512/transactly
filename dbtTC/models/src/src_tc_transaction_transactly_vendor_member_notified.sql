with src_tc_transaction_transactly_vendor_member_notified as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'transaction_transactly_vendor_member_notified') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    member_id
    ,transaction_transactly_vendor_id
from src_tc_transaction_transactly_vendor_member_notified
where _fivetran_deleted = 'FALSE'
