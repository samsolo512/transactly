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

    ,src_tc_user as(
        select *
        from {{ ref('src_tc_user') }}
    )

    ,src_tc_order as(
        select *
        from {{ ref('src_tc_order') }}
    )

    ,src_tc_contract as(
        select *
        from {{ ref('src_tc_contract') }}
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
            ,t.side_id
            ,max(t.status_changed_date) as status_changed_date
        from
            src_tc_transaction t
            join src_tc_address a on t.address_id = a.address_id
        group by street, city, state, t.side_id
    )

    ,final as(
        select
            working.seq_dim_transaction.nextval as transaction_pk
            ,t.transaction_id

            -- agent
            ,agt.first_name as agent_first_name
            ,agt.last_name as agent_last_name
            ,agt.email as agent_email
            ,agt.phone as agent_phone

            -- tc_agent
            ,tc_agt.user_id as tc_agent_user_id
            ,tc_agt.first_name as tc_agent_first_name
            ,tc_agt.last_name as tc_agent_last_name
            ,tc_agt.email as tc_agent_email
            ,tc_agt.phone as tc_agent_phone

            -- created by
            ,t.created_by_id
            ,cbu.first_name as created_by_first_name
            ,cbu.last_name as created_by_last_name
            ,cbu.fullname as created_by_name

            -- transaction
            ,ts.status as transaction_status
            ,t.type_id
            ,t.category_id
            ,t.created_date
            ,t.closed_date
            ,a.street
            ,a.city
            ,a.state
            ,a.zip
            ,case when diy.transaction_id is not null then 1 else 0 end as diy_flag
            ,t.current_contract_id
            ,cont.contract_closing_date
            ,case
                when datediff(day, getdate(), cont.contract_closing_date) >= 0
                then datediff(day, getdate(), cont.contract_closing_date)
                else null
                end as days_to_close
            ,case
                when datediff(day, getdate(), cont.contract_closing_date) between 20 and 50
                then 1
                else 0
                end as days_to_close_20_to_50_flag
            ,cont.contract_amount
            ,ord.order_side_id
            ,case
                when ord.order_side_id = 1 then 'buyer'
                when ord.order_side_id = 2 then 'seller'
                else null
                end as order_side
            ,t.side_id as transaction_side_id
            ,p.party_name as transaction_side
            ,ord.order_status

        from
            src_tc_transaction t
            join src_tc_address a on t.address_id = a.address_id
            join no_dups nd
                on a.street = nd.street
                and a.city = nd.city
                and a.state = nd.state
                and t.side_id = nd.side_id
                and t.status_changed_date = nd.status_changed_date
            left join src_tc_transaction_status ts on t.status_id = ts.transaction_status_id
            left join src_tc_user cbu on t.created_by_id = cbu.user_id
            left join src_tc_party p on t.side_id = p.party_id
            left join diy
                on t.transaction_id = diy.transaction_id
                and t.created_by_id = diy.agent_id
            left join src_tc_contract cont on t.current_contract_id = cont.contract_id
            left join src_tc_order ord on t.transaction_id = ord.transaction_id
            left join src_tc_user tc_agt on ord.assigned_tc_id = tc_agt.user_id
            left join src_tc_user agt on ord.agent_id = agt.user_id
    )

select * from final
