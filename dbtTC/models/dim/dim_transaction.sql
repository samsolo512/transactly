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

    ,src_tc_transaction_status as(
        select *
        from {{ ref('src_tc_transaction_status') }}
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

    ,no_dups as(
        select
            a.street
            ,a.city
            ,a.state
            ,max(t.status_changed_date) as status_changed_date
        from
            src_tc_transaction t
            join src_tc_address a on t.address_id = a.address_id
        group by street, city, state
    )

    ,final as(
        select
            working.seq_dim_transaction.nextval as transaction_pk
            ,t.transaction_id
            ,t.user_id
            ,ts.status
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
            join src_tc_address a on t.address_id = a.address_id
            join no_dups nd
                on a.street = nd.street
                and a.city = nd.city
                and a.state = nd.state
                and t.status_changed_date = nd.status_changed_date
            join src_tc_transaction_status ts on t.status_id = ts.transaction_status_id
            left join src_tc_party p on t.side_id = p.party_id
            left join diy
                on t.transaction_id = diy.transaction_id
                and t.created_by_id = diy.agent_id
    )

select * from final