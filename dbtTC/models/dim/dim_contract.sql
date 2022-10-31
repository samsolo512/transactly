with
    src_tc_contract as(
        select *
        from {{ ref('src_tc_contract') }}
    )

select
    working.seq_dim_contract.nextval as contract_pk
    ,c.contract_id
    ,c.transaction_id
    ,c.contract_amount
    ,c.contract_closing_date
from
    src_tc_contract c
