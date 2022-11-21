with src_tc_contract as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'contract') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    c.id as contract_id
    ,c.transaction_id
    ,c.amount as contract_amount
    ,cast(c.closing_date as date) as contract_closing_date
from src_tc_contract c
where _fivetran_deleted = 'FALSE'
