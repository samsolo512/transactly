with src_tc_contract as(
    select *
    from fivetran.transactly_app_production_rec_accounts.contract
    where lower(_fivetran_deleted) = 'false'
)

select
    c.id as contract_id
    ,c.transaction_id
    ,c.amount as contract_amount
    ,c.closing_date
from src_tc_contract c
