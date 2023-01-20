-- fact_order
-- 1 row/line item
-- this is a combination of the two original TC views:
-- client_orders
-- client_revenue

with
    src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction')}}
    )

    ,src_tc_order as(
        select *
        from {{ ref('src_tc_order')}}
    )

    ,dim_order as(
        select *
        from {{ ref('dim_order')}}
    )

    ,dim_date as(
        select *
        from {{ ref('dim_date')}}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction')}}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user')}}
    )

--     ,order_sequence as(
--         select
--             o.order_id
--             ,user.user_id
--             ,t.created_date
--             ,t.closed_date
--             ,case
--                 when t.closed_date is not null then row_number() over (partition by user.user_id order by t.closed_date, t.transaction_id)
--                 else null end as closed_sequence
--             ,row_number() over (partition by user.user_id order by t.created_date, t.transaction_id) as placed_sequence
--         from
--             src_tc_transaction t
--             join src_tc_order o on t.transaction_id = o.transaction_id
--             left join dim_user user on o.agent_id = user.user_id
--         --order by user.user_id, t.created_date, t.transaction_id
--     )

    ,final as(
        -- transaction.user_id = order.assigned_tc_id
        -- order.agent_id = line_item.user_id

        select
            -- grain
            ord.order_pk

            ,assigned_tc.user_pk as assigned_tc_pk
            ,agent.user_pk as user_pk
            ,created_by.user_pk as created_by_pk
            ,ta.transaction_pk

            -- dates
            ,nvl(created_date.date_pk, 0) as created_date_pk
            ,nvl(closed_date.date_pk, 0) as closed_date_pk

            -- flags
            ,case when closed_date.date_id is not null then 1 else 0 end as closed_date_flag
            ,case when o.assigned_tc_id is not null then 1 else 0 end as assigned_tc_flag

            -- misc
            ,datediff(day, o.created_date, t.created_date) as order_transact_start_lag
--             ,os.closed_sequence
--             ,os.placed_sequence

        from
            src_tc_transaction t
            join src_tc_order o on t.transaction_id = o.transaction_id
            join dim_transaction ta on t.transaction_id = ta.transaction_id
--             left join order_sequence os on o.order_id = os.order_id
            left join dim_order ord on o.order_id = ord.order_id
            left join dim_date created_date on cast(o.created_date as date) = created_date.date_id
            left join dim_date closed_date on cast(t.closed_date as date) = closed_date.date_id
            left join dim_user assigned_tc on o.assigned_tc_id = assigned_tc.user_id
            left join dim_user agent on o.agent_id = agent.user_id
            left join dim_user created_by on t.created_by_id = created_by.user_id

        order by user_pk, created_date.date_pk, closed_date.date_pk
    )

select * from final
