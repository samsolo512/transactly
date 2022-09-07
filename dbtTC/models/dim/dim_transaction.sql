with
    src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,src_tc_address as(
        select *
        from {{ ref('src_tc_address') }}
    )

select
    working.seq_dim_transaction.nextval as transaction_pk
    ,t.transaction_id
    ,t.user_id
    ,t.status_id
    ,t.type_id
    ,t.side_id
    ,t.category_id
    ,t.created_date
    ,t.closed_date
    ,t.created_by_id
    ,a.street
    ,a.city
    ,a.state
    ,a.zip
from
    src_tc_transaction t
    left join src_tc_address a on t.address_id = a.address_id