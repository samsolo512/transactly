with
    src_tc_user as(
        select *
        from {{ ref('src_tc_user') }}
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

    ,src_tc_user_subscription as(
        select *
        from {{ ref('src_tc_user_subscription') }}
    )

    ,hs_agent as(
        select *
        from {{ ref('HS_agent') }}
    )

    ,last_order_placed as (
        select
            l.user_id
            ,max(l.created_date) as last_order_placed
        from
            src_tc_user u
            join src_tc_line_item l on l.user_id = u.user_id
        where
            l.user_id = u.user_id
            and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
            and l.status not in ('withdrawn', 'cancelled')
        group by l.user_id
    )

    ,first_order_placed as (
        select
            l.user_id
            ,min(l.created_date) as first_order_placed
        from
            src_tc_user u
            join src_tc_line_item l on l.user_id = u.user_id
        where
            l.user_id = u.user_id
            and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
            and l.status not in ('withdrawn', 'cancelled')
        group by l.user_id
    )

    ,fifth_order as(
        select * from(
            select
                l.user_id
                ,l.due_date
                ,row_number() over (partition by l.user_id order by l.due_date) as row_num
            from
                src_tc_user u
                join src_tc_line_item l on l.user_id = u.user_id
            where
                l.due_date is not null
                and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
            )
        where row_num = 5
    )

    ,first_order_closed as (
        select
            l.user_id
            ,min(t.closed_date) as first_order_closed
        from
            src_tc_user u
            join src_tc_line_item l on l.user_id = u.user_id
            join src_tc_order o on l.order_id = o.order_id
            join src_tc_transaction t on o.transaction_id = t.transaction_id
        where
            l.user_id = u.user_id
            and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
            and l.status not in ('withdrawn', 'cancelled')
        group by l.user_id
    )

    ,second_order_closed as(
        select *
        from(
            select
                user_id
                ,transaction_id
                ,closed_date as second_order_closed
                ,row_number() over (partition by user_id order by closed_date, transaction_id) as row_num
            from(
                select distinct
                    l.user_id
                    ,t.transaction_id
                    ,t.closed_date
                from
                    src_tc_user u
                    join src_tc_line_item l on l.user_id = u.user_id
                    join src_tc_order o on l.order_id = o.order_id
                    join src_tc_transaction t on o.transaction_id = t.transaction_id
                where
                    t.closed_date is not null
                    and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
                    and l.status not in ('withdrawn', 'cancelled')
            )
        )
        where row_num = 2
    )

    ,third_order_closed as(
        select *
        from(
            select
                user_id
                ,transaction_id
                ,closed_date as third_order_closed
                ,row_number() over (partition by user_id order by closed_date, transaction_id) as row_num
            from(
                select distinct
                    l.user_id
                    ,t.transaction_id
                    ,t.closed_date
                from
                    src_tc_user u
                    join src_tc_line_item l on l.user_id = u.user_id
                    join src_tc_order o on l.order_id = o.order_id
                    join src_tc_transaction t on o.transaction_id = t.transaction_id
                where
                    t.closed_date is not null
                    and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
                    and l.status not in ('withdrawn', 'cancelled')
            )
        )
        where row_num = 3
    )

    ,fourth_order_closed as(
        select *
        from(
            select
                user_id
                ,transaction_id
                ,closed_date as fourth_order_closed
                ,row_number() over (partition by user_id order by closed_date, transaction_id) as row_num
            from(
                select distinct
                    l.user_id
                    ,t.transaction_id
                    ,t.closed_date
                from
                    src_tc_user u
                    join src_tc_line_item l on l.user_id = u.user_id
                    join src_tc_order o on l.order_id = o.order_id
                    join src_tc_transaction t on o.transaction_id = t.transaction_id
                where
                    t.closed_date is not null
                    and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
                    and l.status not in ('withdrawn', 'cancelled')
            )
        )
        where row_num = 4
    )

    ,fifth_order_closed as(
        select *
        from(
            select
                user_id
                ,transaction_id
                ,closed_date as fifth_order_closed
                ,row_number() over (partition by user_id order by closed_date, transaction_id) as row_num
            from(
                select distinct
                    l.user_id
                    ,t.transaction_id
                    ,t.closed_date
                from
                    src_tc_user u
                    join src_tc_line_item l on l.user_id = u.user_id
                    join src_tc_order o on l.order_id = o.order_id
                    join src_tc_transaction t on o.transaction_id = t.transaction_id
                where
                    t.closed_date is not null
                    and l.description in ('Listing Coordination Fee', 'Transaction Coordination Fee')
                    and l.status not in ('withdrawn', 'cancelled')
            )
        )
        where row_num = 5
    )

    ,client as(
        select u.user_id
        from
            src_tc_user u
            join src_tc_line_item li on li.user_id = u.user_id
        where
            u.is_tc_client = 1
            and li.status not in ('withdrawn', 'cancelled')
            and li.due_date is not null
            and lower(li.description) like ('%coordination fee')

        -- users without orders
        union
        select u.user_id
        from
            src_tc_user u
            left join src_tc_order o on u.user_id = o.agent_id
        where
            u.is_tc_client = 1
            and o.agent_id is null
    )

    ,final as(
        select
            working.seq_dim_user.nextval as user_pk
            ,user_id
            ,first_name
            ,last_name
            ,fullname
            ,email
            ,brokerage
            ,subscription_level
            ,lead_status as transaction_coordinator_status
            ,contact_owner
            ,address
            ,address2

            --flags
            ,pays_at_title_flag
            ,eligible_for_clients_flag
            ,tc_staff_flag
            ,tc_client_flag
            ,diy_flag
            ,self_procured_flag

            -- dates
            ,start_date
            ,days_between_start_date_and_first_order_date
            ,max(last_order_due) as last_order_due
            ,tier_3
            ,max(tier_2) as tier_2
            ,max(tier_1) as tier_1

            -- orders placed
            ,max(first_order_placed) as first_order_placed
            ,max(last_order_placed) as last_order_placed

            -- orders closed
            ,max(first_order_closed) as first_order_closed
            ,max(second_order_closed) as second_order_closed
            ,max(third_order_closed) as third_order_closed
            ,max(fourth_order_closed) as fourth_order_closed
            ,max(fifth_order_closed) as fifth_order_closed

        from(
            select
                u.user_id
                ,replace(u.first_name, '"', '') as first_name
                ,replace(u.last_name, '"', '') as last_name
                ,replace(u.fullname, '"', '') as fullname
                ,replace(u.email, '"', '') as email
                ,u.brokerage
                ,sub.subscription_level
                ,case lower(hagent.lead_status)
                    when 'onboarded' then 'Onboarded (TC/Staff)'
                    when 'connected' then 'Closed (TC/Staff)'
                    when 'inactive' then 'Onboarded (TC/Staff)'
                    when 'disqualified' then 'Disqualified (not an agent)'
                    when 'bad' then 'Bad Contact Information'
                    else hagent.lead_status
                    end as lead_status
                ,concat(firstname, ' ', lastname) as contact_owner
                ,hagent.address
                ,hagent.address2

                --flags
                ,case u.pays_at_title
                    when 'TRUE' then 1
                    when 'FALSE' then 0
                    else null
                    end as pays_at_title_flag
                ,case lower(hagent.eligible_for_clients)
                    when 'yes' then 1
                    when 'no' then 0
                    else null
                    end as eligible_for_clients_flag
                ,case when hagent.type = 'TC Staff' then 1 else 0 end as tc_staff_flag
                ,case when c.user_id is not null then 1 else 0 end as tc_client_flag
                ,case when u.is_tc_client = 'FALSE' then 1 else 0 end as diy_flag
                ,case
                    when u.self_procured = 'TRUE' then 1
                    when u.self_procured = 'FALSE' then 0
                    else null
                    end as self_procured_flag

                -- dates
                ,hagent.created_date as start_date
                ,datediff(day, hagent.created_date, fp.first_order_placed) as days_between_start_date_and_first_order_date
                ,fp.first_order_placed as tier_3
                ,min(li.due_date) as tier_2
                ,fifth.due_date as tier_1
                ,max(li.due_date) as last_order_due

                -- placed orders
                ,fp.first_order_placed
                ,loc.last_order_placed

                -- closed orders
                ,c1.first_order_closed
                ,c2.second_order_closed
                ,c3.third_order_closed
                ,c4.fourth_order_closed
                ,c5.fifth_order_closed

            from
                src_tc_user u
                left join client c on u.user_id = c.user_id
                left join src_tc_line_item li on li.user_id = u.user_id
                left join hs_agent hagent on u.user_id = hagent.transactly_id
                left join src_tc_user_subscription sub on u.user_id = sub.user_id
                left join fifth_order fifth on u.user_id = fifth.user_id
                left join src_hs_owners c_owner on hagent.contact_owner = c_owner.ownerid

                -- placed orders
                left join first_order_placed fp on u.user_id = fp.user_id
                left join last_order_placed loc on u.user_id = loc.user_id

                -- closed orders
                left join first_order_closed c1 on u.user_id = c1.user_id
                left join second_order_closed c2 on u.user_id = c2.user_id
                left join third_order_closed c3 on u.user_id = c3.user_id
                left join fourth_order_closed c4 on u.user_id = c4.user_id
                left join fifth_order_closed c5 on u.user_id = c5.user_id

            group by u.user_id, replace(u.first_name, '"', ''), replace(u.last_name, '"', ''), replace(u.fullname, '"', ''), replace(u.email, '"', ''), u.brokerage, pays_at_title_flag, tc_client_flag, self_procured_flag, tier_3, loc.last_order_placed, fp.first_order_placed, c1.first_order_closed, c2.second_order_closed, c3.third_order_closed, c4.fourth_order_closed, c5.fifth_order_closed, fifth.due_date, sub.subscription_level, hagent.lead_status, hagent.eligible_for_clients, hagent.created_date, days_between_start_date_and_first_order_date, tc_staff_flag, diy_flag, concat(firstname, ' ', lastname), hagent.address, hagent.address2
        )

        group by user_pk, user_id, first_name, last_name, fullname, email, brokerage, pays_at_title_flag, tc_client_flag, tier_3, subscription_level, lead_status, eligible_for_clients_flag, self_procured_flag, start_date, days_between_start_date_and_first_order_date, tc_staff_flag, diy_flag, contact_owner, address, address2

        union select 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null
    )

select * from final