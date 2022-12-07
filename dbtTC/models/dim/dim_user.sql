-- dim_user

with
    src_tc_user as(
        select *
        from {{ ref('src_tc_user') }}
    )

    ,src_tc_user_role as(
        select *
        from {{ ref('src_tc_user_role') }}
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

    ,src_tc_office as(
        select *
        from {{ ref('src_tc_office') }}
    )

    ,src_tc_user_subscription as(
        select *
        from {{ ref('src_tc_user_subscription') }}
    )

    ,hs_agent as(
        select *
        from {{ ref('HS_agent') }}
    )

    ,src_tc_office_user as(
        select *
        from {{ ref('src_tc_office_user') }}
    )

    ,src_hs_owners as(
        select *
        from {{ ref('src_hs_owners') }}
    )

    ,src_tc_user_agent_subscription_tier as(
        select *
        from {{ ref('src_tc_user_agent_subscription_tier') }}
    )

    ,src_tc_user_transactly_vendor_opt_out as(
        select *
        from {{ ref('src_tc_user_transactly_vendor_opt_out') }}
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
        select distinct
            u.user_id
            ,u.email
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
        select distinct
            u.user_id
            ,u.email
        from
            src_tc_user u
            left join src_tc_order o on u.user_id = o.agent_id
        where
            u.is_tc_client = 1
            and o.agent_id is null
    )

    ,combine as(
        -- user
        select
            u.email
            ,case when c.user_id is not null then 1 else 0 end as client_flag
            ,0 as lead_flag
        from
            src_tc_user u
            left join client c on u.user_id = c.user_id
    )

    ,client_flags as(
        select
            email
            ,sum(client_flag) as client_flag
            ,sum(lead_flag) as lead_flag
        from
            combine
        group by email
    )

    ,user_lead as(
        select
            u.user_id
            ,u.first_name
            ,u.last_name
            ,u.fullname
            ,u.email
            ,u.created_date as tc_created_date
            ,c.client_flag
            ,c.lead_flag
        from
            client_flags c
            left join src_tc_user u on c.email = u.email
    )

    ,max_tier as(
        select
            user_id
            ,max(start_date) as start_date
        from src_tc_user_agent_subscription_tier
        group by
            user_id
    )

    ,subscrip_tier as(
        select distinct
            a.user_id
            ,a.start_date
            ,case agent_subscription_tier_id
                when 1 then 'Basic'
                when 2 then 'Pro'
                when 3 then 'Pro Plus'
                else null
                end as tier
        from
            src_tc_user_agent_subscription_tier a
            join max_tier mt
                on a.user_id = mt.user_id
                and a.start_date = mt.start_date
    )

    -- roles
    ,user_role as(
        select
            usr.user_id
            ,r.role
            ,r.role_name
        from
            src_tc_user usr
            join src_tc_user_role ur on usr.user_id = ur.user_id
            join src_tc_role r on ur.role_id = r.role_id
    )

    ,role_pivot as(
        select *
        from
            user_role pivot(
                count(role)
                for role in(
                    'ROLE_SUPER_ADMIN', 'ROLE_USER', 'ROLE_STAFF', 'ROLE_BETA_AGENT'
                    ,'ROLE_AGENT', 'ROLE_UNLICENSED', 'ROLE_LENDER', 'ROLE_PARTNER', 'ROLE_TRANS_COORDINATOR'
                )
            ) p
                (user_id, role_name, role_super_admin, role_user, role_staff, role_beta_agent
                 ,role_agent, role_unlicensed, role_lender, role_partner, role_trans_coordinator)
        order by user_id
    )

    ,role_combine as(
        select
            user_id
            ,sum(role_super_admin) as role_super_admin
            ,sum(role_user) as role_user
            ,sum(role_staff) as role_staff
            ,sum(role_beta_agent) as role_beta_agent
            ,sum(role_agent) as role_agent
            ,sum(role_unlicensed) as role_unlicensed
            ,sum(role_lender) as role_lender
            ,sum(role_partner) as role_partner
            ,sum(role_trans_coordinator) as role_trans_coordinator
        from role_pivot
        group by user_id
        order by user_id
    )

    -- utility transfer
    ,util as(
        select *
        from
            src_tc_user_transactly_vendor_opt_out uto
        where
            uto.vendor_type_id = 10  -- utility transfer
    )

    -- user office
    ,offc as(
        select distinct
            ou.user_id
            ,nvl(o.parent_office_id, o.office_id) as office_id
            ,o.office_name
            ,o.parent_office_id
            ,po.office_name as parent_office
            ,ou.created as created_date
        from
            src_tc_office_user ou
            join src_tc_office o on ou.office_id = o.office_id
            left join src_tc_office po on o.parent_office_id = po.office_id
        where
            lower(o.office_name) not like '%test%'
    )

    ,unique_office as(
        select
            a.user_id
            ,max(a.created_date) as created_date
        from
            offc a
        group by
            a.user_id
    )

    ,user_office as(
        select
            a.user_id
            ,a.office_id
            ,a.office_name
        from
            offc a
            join unique_office b
                on a.user_id = b.user_id
                and a.created_date = b.created_date
    )
    
    ,order_sequence as(
        select
            user.user_id
            ,o.order_id
            ,l.created_date
            ,l.due_date as closed_date
            ,l.id as line_item_id
            ,case
                when l.due_date is not null then row_number() over (partition by user.user_id order by l.due_date, o.order_id)
                else null 
                end as closed_sequence
            ,row_number() over (partition by user.user_id order by l.created_date, o.order_id) as placed_sequence

        from
            src_tc_transaction t
            join src_tc_order o on t.transaction_id = o.transaction_id
            left join src_tc_line_item l
                join dim_line_item line on l.id = line.line_item_id
            on o.order_id = l.order_id
            left join dim_user user on o.agent_id = user.user_id

        where
            l.description in('Listing Coordination Fee', 'Transaction Coordination Fee')
    )
    
    ,total_orders as(
        select
            user_id
            ,max(closed_sequence) as total_closed_orders
        from
            order_sequence os
        group by
            user_id
    )

    ,final_logic as(
        select
            u.user_id
            ,nvl(replace(u.first_name, '"', ''), ul.first_name) as first_name
            ,nvl(replace(u.last_name, '"', ''), ul.last_name) as last_name
            ,nvl(replace(u.fullname, '"', ''), ul.fullname) as fullname
            ,nvl(replace(u.email, '"', ''), ul.email) as email
            ,u.brokerage
            ,uo.office_id
            ,uo.office_name
            ,st.tier as subscription_level
            ,case lower(hagent.lead_status)
                when 'onboarded' then 'Onboarded (TC/Staff)'
                when 'connected' then 'Closed (TC/Staff)'
                when 'inactive' then 'Onboarded (TC/Staff)'
                when 'disqualified' then 'Disqualified (not an agent)'
                when 'bad' then 'Bad Contact Information'
                else hagent.lead_status
                end as lead_status
            ,concat(cont_owner.firstname, ' ', cont_owner.lastname) as contact_owner
            ,concat(orig_agent.firstname, ' ', orig_agent.lastname) as original_sales_rep_name
            ,u.stripe_account_id
            ,case when util.user_id is null then 'IN' else 'OUT' end as utility_opt_in_status

            -- roles
            ,rc.role_super_admin as role_super_admin_flag
            ,rc.role_user as role_user_flag
            ,rc.role_staff as role_staff_flag
            ,rc.role_beta_agent as role_beta_agent_flag
            ,rc.role_agent as role_agent_flag
            ,rc.role_unlicensed as role_unlicensed_flag
            ,rc.role_lender as role_lender_flag
            ,rc.role_partner as role_partner_flag
            ,rc.role_trans_coordinator as role_trans_coordinator_flag

            -- agent address
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
            ,ul.lead_flag
            ,case
                when ul.lead_flag = 0 and tc_client_flag = 1 then 'TC only'
                when ul.lead_flag = 1 and tc_client_flag = 0 then 'SF only'
                when ul.lead_flag = 1 and tc_client_flag = 1 then 'TC and SF'
                when ul.lead_flag = 0 and tc_client_flag = 0 then 'TC only'
                when ul.lead_flag = 0 and tc_client_flag = 0 and u.user_id is not null and u.user_id <> 0 then 'TC only'
                else null
                end as client_type
            ,case
                when u.self_procured = 'TRUE' then 1
                when u.self_procured = 'FALSE' then 0
                else null
                end as self_procured_flag
            ,u.is_active as active_flag

            --HubSpot fields
            ,hagent.type as HS_agent_type
            ,hagent.transactly_home_insurance_vendor_status
            ,hagent.transactly_utility_connection_vendor_status

            -- dates
            ,u.created_date as user_created_date
            ,hagent.created_date as start_date
            ,datediff(day, hagent.created_date, fp.first_order_placed) as days_between_start_date_and_first_order_date
            ,fp.first_order_placed as tier_3
            ,min(li.due_date) as tier_2
            ,fifth.due_date as tier_1
            ,max(li.due_date) as last_order_due

            -- orders
            ,fp.first_order_placed
            ,loc.last_order_placed
            ,c1.first_order_closed
            ,c2.second_order_closed
            ,c3.third_order_closed
            ,c4.fourth_order_closed
            ,c5.fifth_order_closed
        
            ,dateadd(year, 1, fp.first_order_placed) as anniversary_1_yr_1st_order_placed
            ,datediff(day, loc.last_order_placed, getdate()) as days_since_last_order_placed
            ,case when days_since_last_order_placed >= 90 then 1 else 0 end as days_since_last_order_placed_over_90_flag
            ,toto.total_closed_orders

        from
            user_lead ul

            -- TC user
            left join src_tc_user u on u.user_id = ul.user_id
            left join client c on u.user_id = c.user_id
            left join src_tc_line_item li on li.user_id = u.user_id
            left join hs_agent hagent on u.user_id = hagent.transactly_id
            left join src_tc_user_subscription sub on u.user_id = sub.user_id
            left join fifth_order fifth on u.user_id = fifth.user_id
            left join src_hs_owners orig_agent on hagent.original_sales_rep = cast(orig_agent.ownerid as varchar)
            left join src_hs_owners cont_owner on hagent.contact_owner = cast(cont_owner.ownerid as varchar)
            left join subscrip_tier st on u.user_id = st.user_id
            left join role_combine rc on ul.user_id = rc.user_id
            left join user_office uo on u.user_id = uo.user_id
            left join util on ul.user_id = util.user_id

            -- orders
            left join first_order_placed fp on u.user_id = fp.user_id
            left join last_order_placed loc on u.user_id = loc.user_id
            left join first_order_closed c1 on u.user_id = c1.user_id
            left join second_order_closed c2 on u.user_id = c2.user_id
            left join third_order_closed c3 on u.user_id = c3.user_id
            left join fourth_order_closed c4 on u.user_id = c4.user_id
            left join fifth_order_closed c5 on u.user_id = c5.user_id
            left join total_orders toto on u.user_id = toto.user_id

        group by
            u.user_id
            ,nvl(replace(u.first_name, '"', ''), ul.first_name)
            ,nvl(replace(u.last_name, '"', ''), ul.last_name)
            ,nvl(replace(u.fullname, '"', ''), ul.fullname)
            ,nvl(replace(u.email, '"', ''), ul.email)
            ,u.brokerage
            ,uo.office_id
            ,uo.office_name
            ,st.tier
            ,hagent.lead_status
            ,concat(cont_owner.firstname, ' ', cont_owner.lastname)
            ,u.stripe_account_id
            ,utility_opt_in_status
            ,rc.role_super_admin
            ,rc.role_user
            ,rc.role_staff
            ,rc.role_beta_agent
            ,rc.role_agent
            ,rc.role_unlicensed
            ,rc.role_lender
            ,rc.role_partner
            ,rc.role_trans_coordinator
            ,hagent.address
            ,hagent.address2
            ,concat(orig_agent.firstname, ' ', orig_agent.lastname)
            ,pays_at_title_flag
            ,hagent.eligible_for_clients
            ,tc_staff_flag
            ,tc_client_flag
            ,ul.lead_flag
            ,client_type
            ,self_procured_flag
            ,u.is_active
            ,hagent.type
            ,hagent.transactly_home_insurance_vendor_status
            ,hagent.transactly_utility_connection_vendor_status
            ,u.created_date
            ,hagent.created_date
            ,tier_3
            ,fifth.due_date
            ,fp.first_order_placed
            ,loc.last_order_placed
            ,c1.first_order_closed
            ,c2.second_order_closed
            ,c3.third_order_closed
            ,c4.fourth_order_closed
            ,c5.fifth_order_closed
            ,dateadd(year, 1, fp.first_order_placed)
            ,datediff(day, loc.last_order_placed, getdate())
            ,case when days_since_last_order_placed >= 90 then 1 else 0 end
            ,toto.total_closed_orders
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
            ,office_id
            ,office_name
            ,subscription_level
            ,lead_status as transaction_coordinator_status
            ,contact_owner
            ,stripe_account_id
            ,utility_opt_in_status

            -- roles
            ,role_super_admin_flag
            ,role_user_flag
            ,role_staff_flag
            ,role_beta_agent_flag
            ,role_agent_flag
            ,role_unlicensed_flag
            ,role_lender_flag
            ,role_partner_flag
            ,role_trans_coordinator_flag

            -- agent address
            ,address
            ,address2
            ,original_sales_rep_name

            --flags
            ,pays_at_title_flag
            ,eligible_for_clients_flag
            ,tc_staff_flag
            ,tc_client_flag
            ,lead_flag
            ,client_type
            ,self_procured_flag
            ,active_flag

            --HubSpot fields
            ,HS_agent_type
            ,transactly_home_insurance_vendor_status
            ,transactly_utility_connection_vendor_status

            -- dates
            ,user_created_date
            ,start_date
            ,days_between_start_date_and_first_order_date
            ,tier_3
            ,max(tier_2) as tier_2
            ,max(tier_1) as tier_1
            ,last_order_due

            -- orders
            ,max(first_order_placed) as first_order_placed
            ,max(last_order_placed) as last_order_placed
            ,max(first_order_closed) as first_order_closed
            ,max(second_order_closed) as second_order_closed
            ,max(third_order_closed) as third_order_closed
            ,max(fourth_order_closed) as fourth_order_closed
            ,max(fifth_order_closed) as fifth_order_closed
        
            ,anniversary_1_yr_1st_order_placed
            ,days_since_last_order_placed
            ,days_since_last_order_placed_over_90_flag
            ,total_closed_orders

        from final_logic

        group by
            user_pk
            ,user_id
            ,first_name
            ,last_name
            ,fullname
            ,email
            ,brokerage
            ,office_id
            ,office_name
            ,subscription_level
            ,lead_status
            ,contact_owner
            ,stripe_account_id
            ,utility_opt_in_status
            ,role_super_admin_flag
            ,role_user_flag
            ,role_staff_flag
            ,role_beta_agent_flag
            ,role_agent_flag
            ,role_unlicensed_flag
            ,role_lender_flag
            ,role_partner_flag
            ,role_trans_coordinator_flag
            ,address
            ,address2
            ,original_sales_rep_name
            ,pays_at_title_flag
            ,eligible_for_clients_flag
            ,tc_staff_flag
            ,tc_client_flag
            ,lead_flag
            ,client_type
            ,self_procured_flag
            ,active_flag
            ,HS_agent_type
            ,transactly_home_insurance_vendor_status
            ,transactly_utility_connection_vendor_status
            ,user_created_date
            ,start_date
            ,days_between_start_date_and_first_order_date
            ,tier_3
            ,last_order_due
            ,anniversary_1_yr_1st_order_placed
            ,days_since_last_order_placed
            ,days_since_last_order_placed_over_90_flag
            ,total_closed_orders


        union select
            0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null

    )

select * from final


/*
 -- check for dups in Hubsot
select transactly_id, count(1)
from hs_agent
where transactly_id is not null
group by transactly_id
having count(1) > 1
order by count(1) desc
 */