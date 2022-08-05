with
    src_tc_contract as(
        select *
        from {{ ref('src_tc_contract') }}
    )

select
    c.contract_id
    ,c.transaction_id
    ,c.contract_amount
    ,c.closing_date
from
    src_tc_contract c
