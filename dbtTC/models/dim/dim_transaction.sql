with
    src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,src_tc_address as(
        select *
        from {{ ref('src_tc_address') }}
    )

    ,src_tc_address as(
        select *
        from {{ ref('src_tc_address') }}
    )

    ,src_tc_party as(
        select *
        from {{ ref('src_tc_party') }}
    )

    ,diy as(
        select
            t.transaction_id
            ,t.created_by_id as agent_id
        from src_tc_transaction t
        where
            t.status_id not in (5, 6, 7)
            and t.transaction_id not in(
                select transaction_id
                from src_tc_order
                where transaction_id is not null
            )
    )

    ,final as(
        select
            working.seq_dim_transaction.nextval as transaction_pk
            ,t.transaction_id
            ,t.user_id
            ,t.status_id
            ,t.type_id
            ,p.party_name as side_id
            ,t.category_id
            ,t.created_date
            ,t.closed_date
            ,t.created_by_id
            ,a.street
            ,a.city
            ,a.state
            ,a.zip
            ,case when diy.transaction_id is not null then 1 else 0 end as diy_flag
        from
            src_tc_transaction t
            left join src_tc_address a on t.address_id = a.address_id
            left join src_tc_party p on t.side_id = p.party_id
            left join diy
                on t.transaction_id = diy.transaction_id
                and t.created_by_id = diy.agent_id
    )

select * from final