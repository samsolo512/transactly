with
    src_tc_address as(
        select *
        from {{ ref('src_tc_address') }}
    )

    ,src_tc_line_item as(
        select *
        from {{ ref('src_tc_line_item') }}
    )

    ,src_tc_order as(
        select *
        from {{ ref('src_tc_order') }}
    )

    ,src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,dim_line_item as(
        select *
        from {{ ref('dim_line_item') }}
    )

    ,dim_order as(
        select *
        from {{ ref('dim_order') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,cancelled as(
        select
            t.transaction_id
            ,t.created_by_id as agent_id
        from src_tc_transaction t
        where
            t.status_id in (5, 6, 7)
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
            ,t.status_changed_date
            ,o.order_id
            ,t.transaction_id
            ,row_number() over(
            	partition by a.street, a.city, a.state, t.side_id 
                order by t.status_changed_date desc nulls last, o.order_id desc nulls last, t.transaction_id desc nulls last
            ) as rownum
        from
            src_tc_transaction t
            join src_tc_address a on t.address_id = a.address_id
            left join src_tc_order o on t.transaction_id = o.transaction_id
    )

    ,final as(
        select
            transaction.transaction_pk
            ,nvl(user.user_pk, (select user_pk from dim_user where user_id = 0)) as user_pk
            ,nvl(ord.order_pk, (select order_pk from dim_order where order_id = 0)) as order_pk
            ,case when trans.status_id = 3 then 1 else 0 end as closed_flag
            ,case when trans.status_id in (1, 2, 4) then 1 else 0 end as active_flag
            ,case when trans.side_id = 1 then 1 else 0 end as buy_flag
            ,case when trans.side_id <> 1 then 1 else 0 end as sell_flag
            ,case when c.transaction_id is not null then 1 else 0 end as cancelled_flag
            ,trans.created_date as created_date
            ,trans.status_changed_date

            ,sum(l.agent_pays) as agent_pays
            ,sum(l.office_pays) as office_pays
            ,sum(l.agent_pays + l.office_pays) as total_fees

        from
            src_tc_transaction trans
            join src_tc_address a on trans.address_id = a.address_id
            left join src_tc_order o on trans.transaction_id = o.transaction_id
            left join dim_order ord on o.order_id = ord.order_id
            -- add line item in order to get total fees where applicable
            left join src_tc_line_item l
                join dim_line_item line on l.id = line.line_item_id
            on o.order_id = l.order_id
            join no_dups b
                on a.street = b.street
                and a.city = b.city
                and a.state = b.state
                and trans.side_id = b.side_id
                and nvl(trans.status_changed_date, '1900-01-01') = nvl(b.status_changed_date, '1900-01-01')
                and nvl(o.order_id, 1) = nvl(b.order_id, 1)
                and nvl(trans.transaction_id, 1) = nvl(b.transaction_id, 1)
                and b.rownum = 1
            join dim_transaction transaction on trans.transaction_id = transaction.transaction_id
            left join dim_user user on trans.created_by_id = user.user_id
            left join cancelled c
                on trans.transaction_id = c.transaction_id
                and trans.created_by_id = c.agent_id

        group by
            1,2,3,4,5,6,7,8,9,10
    )

select * from final
