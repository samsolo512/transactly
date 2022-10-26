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

    ,src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity') }}
    )

    ,src_sf_contact as(
        select *
        from {{ ref('src_sf_contact') }}
    )

    ,src_tc_user_subscription as(
        select *
        from {{ ref('src_tc_user_subscription') }}
    )

    ,hs_agent as(
        select *
        from {{ ref('HS_agent') }}
    )

    ,src_sf_lead as(
        select *
        from {{ ref('src_sf_lead') }}
    )

    ,src_hs_owners as(
        select *
        from {{ ref('src_hs_owners') }}
    )

    ,src_tc_user_agent_subscription_tier as(
        select *
        from {{ ref('src_tc_user_agent_subscription_tier') }}
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

    ,states as(
        select
            lead_id
            ,case
                when l.state is null and(lower(l.city) like '%atlanta%') then 'GA'
                when l.state is null and(lower(l.city) like '%phoenix%') then 'AZ'
                when l.state is null and(lower(l.city) like '%las vegas%') then 'NV'
                when l.state is null and(lower(l.city) like '%san antonio%') then 'TX'
                when l.state is null and(lower(l.city) like '%charlotte%') then 'NC'
                when l.state is null and(lower(l.city) like '%chicago%') then 'IL'
                when l.state is null and(lower(l.city) like '%denver%') then 'CO'
                when l.state is null and(lower(l.city) like '%los angeles%') then 'CA'
                when l.state is null and(lower(l.city) like '%washington%') then 'BC'
                when l.state is null and(lower(l.city) like '%tampa%') then 'FL'
                when l.state is null and(lower(l.city) like '%orlando%') then 'FL'
                when l.state is null and(lower(l.city) like '%miami%') then 'FL'
                when l.state is null and(lower(l.city) like '%jacksonville%') then 'FL'
                when l.state is null and(lower(l.city) like '%nashville%') then 'TN'
                when l.state is null and(lower(l.city) like '%memphis%') then 'TN'
                when l.state is null and(lower(l.city) like '%san diego%') then 'CA'
                when l.state is null and(lower(l.city) like '%fresno%') then 'CA'
                when l.state is null and(lower(l.city) like '%san francisco%') then 'CA'
                when l.state is null and(lower(l.city) like '%kansas city%') then 'MO'
                when l.state is null and(lower(l.city) like '%philadelphia%') then 'PA'
                when l.state is null and(lower(l.city) like '%pittsburgh%') then 'PA'
                when l.state is null and(lower(l.city) like '%new york%') then 'NY'
                when l.state is null and(lower(l.city) like '%portland%') then 'OR'
                when l.state is null and(lower(l.city) like '%fort worth%') then 'TX'
                when l.state is null and(lower(l.city) like '%seattle%') then 'WA'
                when l.state is null and(lower(l.city) like '%sacramento%') then 'CA'
                when l.state is null and(lower(l.city) like '%detroit%') then 'MI'
                when l.state is null and(lower(l.city) like '%minneapolis%') then 'MN'
                when l.state is null and(lower(l.city) like '%indianapolis%') then 'IN'
                when l.state is null and(lower(l.city) like '%columbus%') then 'OH'
                when l.state is null and(lower(l.city) like '%cincinnati%') then 'OH'
                when l.state is null and(lower(l.city) like '%st. louis%') then 'MO'
                when l.state is null and(lower(l.city) like '%baltimore%') then 'MD'
                when l.state is null and(lower(l.city) like '%baltimore%') then 'AZ'
                when l.state is null and(lower(l.city) like '%albuquerque%') then 'NM'
                when l.state is null and(lower(l.city) like '%tucson%') then 'AZ'
                when l.state is null and(lower(l.city) like '%el paso%') then 'NM'
                when l.state is null and(lower(l.city) like '%riverside%') then 'CA'
                when l.state is null and(lower(l.city) like '%oklahoma city%') then 'OK'
                when l.state is null and(lower(l.city) like '%boston%') then 'MA'
                when l.state is null and(lower(l.city) like '%arlington%') then 'VA'
                when l.state is null and(lower(l.city) like '%roanoke%') then 'VA'
                when l.state is null and(lower(l.city) like '%bedford%') then 'CT'
                when l.state is null and(lower(l.city) like '%plano%') then 'TX'

                when l.state is null and(lower(l.street) like '% al %') then 'AL'
                when l.state is null and(lower(l.street) like '% ar %') then 'AR'
                when l.state is null and(lower(l.street) like '% az %') then 'AZ'
                when l.state is null and(lower(l.street) like '% ca %') then 'CA'
                when l.state is null and(lower(l.street) like '% co %') then 'CO'
                when l.state is null and(lower(l.street) like '%colorado%') then 'CO'
                when l.state is null and(lower(l.street) like '% de %') then 'DE'
                when l.state is null and(lower(l.street) like '% fl %') then 'FL'
                when l.state is null and(lower(l.street) like '% ga %') then 'GA'
                when l.state is null and(lower(l.street) like '% ga') then 'GA'
                when l.state is null and(lower(l.street) like '% hi %') then 'HI'
                when l.state is null and(lower(l.street) like '% id%') then 'ID'
                when l.state is null and(lower(l.street) like '% il %') then 'IL'
                when l.state is null and(lower(l.street) like '% il.%') then 'IL'
                when l.state is null and(lower(l.street) like '%illinois%') then 'IL'
                when l.state is null and(lower(l.street) like '% in %') then 'IN'
                when l.state is null and(lower(l.street) like '% ks %') then 'KS'
                when l.state is null and(lower(l.street) like '% ky %') then 'KY'
                when l.state is null and(lower(l.street) like '% la %') then 'LA'
                when l.state is null and(lower(l.street) like '% ma %') then 'MA'
                when l.state is null and(lower(l.street) like '% md %') then 'MD'
                when l.state is null and(lower(l.street) like '% me %') then 'ME'
                when l.state is null and(lower(l.street) like '% mi %') then 'MI'
                when l.state is null and(lower(l.street) like '% mo %') then 'MO'
                when l.state is null and(lower(l.street) like '%mn%') then 'MN'
                when l.state is null and(lower(l.street) like '% ms %') then 'MS'
                when l.state is null and(lower(l.street) like '%mt%') then 'MT'
                when l.state is null and(lower(l.street) like '% nc %') then 'NC'
                when l.state is null and(lower(l.street) like '% nh %') then 'NH'
                when l.state is null and(lower(l.street) like '% nj %') then 'NJ'
                when l.state is null and(lower(l.street) like '% nv %') then 'NV'
                when l.state is null and(lower(l.street) like '% ny %') then 'NY'
                when l.state is null and(lower(l.street) like '% oh %') then 'OH'
                when l.state is null and(lower(l.street) like '% ok %') then 'OK'
                when l.state is null and(lower(l.street) like '% ok') then 'OK'
                when l.state is null and(lower(l.street) like '% or %') then 'OR'
                when l.state is null and(lower(l.street) like '% or') then 'OR'
                when l.state is null and(lower(l.street) like '% pa %') then 'PA'
                when l.state is null and(lower(l.street) like '% sc %') then 'SC'
                when l.state is null and(lower(l.street) like '% sd %') then 'SD'
                when l.state is null and(lower(l.street) like '%tn%') then 'TN'
                when l.state is null and(lower(l.street) like '% va %') then 'VA'
                when l.state is null and(lower(l.street) like '% vt %') then 'VT'
                when l.state is null and(lower(l.street) like '% wa %') then 'WA'
                when l.state is null and(lower(l.street) like '% wi %') then 'WI'

                when
                    l.state is null
                    and(
                        lower(l.street) like '%dallas%'
                        or lower(l.street) like '%amarillo%'
                        or lower(l.street) like '%austin%'
                        or lower(l.street) like '%houston%'
                        or lower(l.street) like '%leander%'
                        or lower(l.street) like '%belton%'
                        or lower(l.street) like '%lufkin%'
                        or lower(l.street) like '%tx%'
                        or lower(l.street) like '%texas%'
                        or lower(l.street) like '%prosper%'
                        or lower(l.street) like '%kilgore%'

                        or lower(l.city) like '%dallas%'
                        or lower(l.city) like '%amarillo%'
                        or lower(l.city) like '%austin%'
                        or lower(l.city) like '%houston%'
                        or lower(l.city) like '%leander%'
                        or lower(l.city) like '%belton%'
                        or lower(l.city) like '%lufkin%'
                        or lower(l.city) like '%tx%'
                        or lower(l.city) like '%texas%'
                        or lower(l.city) like '%prosper%'
                        or lower(l.city) like '%kilgore%'

                        -- or (l.city is null and l.street is not null and c.lead_account_name like '2TIO%')
                    )
                then 'TX'

                when lower(l.state) = 'texas' then 'TX'
                when lower(l.state) = 'tx' then 'TX'
                when lower(l.state) = 'tx 75495' then 'TX'
                when lower(l.state) = 'north carolina' then 'NC'
                when lower(l.state) = 'arizona' then 'AZ'
                when lower(l.state) = 'georgia' then 'GA'
                when lower(l.state) = 'massachusetts' then 'MA'
                when lower(l.state) = 'colorado' then 'CO'
                when lower(l.state) = 'co' then 'CO'
                when lower(l.state) = 'alabama' then 'AL'
                when lower(l.state) = 'california' then 'CA'
                when lower(l.state) = 'connecticut' then 'CT'
                when lower(l.state) = 'louisiana' then 'LA'
                when lower(l.state) = 'arkansas' then 'AR'
                when lower(l.state) = 'florida' then 'FL'
                when lower(l.state) = 'new york' then 'NY'

                else l.state
                end as state

        from src_sf_lead l
    )

    -- distinct opportunity
    ,distinct_opportunity as(
        select
            o.contact_id
            ,max(o.close_date) as close_date
        from
            src_sf_opportunity o
        group by
            o.contact_id
    )

    -- distinct contact
    ,distinct_contact as(
        select
            c.converted_lead_c
            ,max(c.created_date_time) as created_date_time
        from
            src_sf_contact c
            left join src_sf_opportunity o
                join distinct_opportunity do on o.contact_id = do.contact_id
            on c.contact_id = o.contact_id
        group by c.converted_lead_c
    )

    ,all_contact as(
        select
            c.*
            ,a.account_name
        from
            src_sf_contact c
            join distinct_contact dc
                on c.converted_lead_c = dc.converted_lead_c
                and c.created_date_time = dc.created_date_time
            left join src_sf_account a on c.account_id = a.account_id
    )

    -- lead
    ,lead_date as(
        select
            l.email
            ,case when cont.converted_lead_c is not null then 1 else 0 end as contact_flag
            ,max(l.created_date_time) as created_date_time
        from
            src_sf_lead l
            left join all_contact cont on l.lead_id = cont.converted_lead_c
        group by l.email, cont.converted_lead_c
    )

    ,lead_max_contact as(
        select
            ld.email
            ,max(ld.contact_flag) as contact_flag
        from
            src_sf_lead l
            join lead_date ld on l.email = ld.email
        group by ld.email
    )

    ,lead_id as(
        select
            l.email
            ,min(l.lead_id) as lead_id
        from
            src_sf_lead l
            join lead_date ld on l.email = ld.email
            join lead_max_contact lmc
                on l.email = lmc.email
                and ld.contact_flag = lmc.contact_flag
        group by l.email
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

        -- lead
        union
        select
            email
            ,0 as client_flag
            ,1 as lead_flag
        from
            lead_id
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
            ,l.lead_id
            ,nvl(u.first_name, l.first_name) as first_name
            ,nvl(u.last_name, l.last_name) as last_name
            ,nvl(u.fullname, l.name) as fullname
            ,nvl(u.email, l.email) as email
            ,l.street
            ,l.city
            ,l.state
            ,l.country
            ,l.zip
            ,l.phone
            ,u.created_date as tc_created_date
            ,l.created_date as lead_created_date
            ,c.client_flag
            ,c.lead_flag
            ,l.lead_source
            ,l.agent_name
            ,l.agent_email
        from
            client_flags c
            left join src_tc_user u on c.email = u.email
            left join src_sf_lead l
                join lead_id ld
                    on l.email = ld.email
                    and l.lead_id = ld.lead_id
                on c.email = l.email
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

    ,final_logic as(
        select
            u.user_id
            ,ul.lead_id
            ,nvl(replace(u.first_name, '"', ''), ul.first_name) as first_name
            ,nvl(replace(u.last_name, '"', ''), ul.last_name) as last_name
            ,nvl(replace(u.fullname, '"', ''), ul.fullname) as fullname
            ,nvl(replace(u.email, '"', ''), ul.email) as email
            ,u.brokerage
            ,st.tier as subscription_level
            ,case lower(hagent.lead_status)
                when 'onboarded' then 'Onboarded (TC/Staff)'
                when 'connected' then 'Closed (TC/Staff)'
                when 'inactive' then 'Onboarded (TC/Staff)'
                when 'disqualified' then 'Disqualified (not an agent)'
                when 'bad' then 'Bad Contact Information'
                else hagent.lead_status
                end as lead_status
            ,concat(cont.first_name, ' ', cont.last_name) as contact_owner
            ,concat(orig_agent.firstname, ' ', orig_agent.lastname) as original_sales_rep_name
            ,ul.lead_source
            ,cont.contact_id
            ,u.stripe_account_id

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
            ,ul.agent_name
            ,ul.agent_email
            ,hagent.address
            ,hagent.address2

            -- lead address
            ,ul.street as lead_street
            ,ul.city as lead_city
            ,states.state as lead_state
            ,ul.zip as lead_zip
            ,ul.country as lead_country
            ,concat(
                ul.street
                ,case when ul.city is not null then ', ' || ul.city else '' end
                ,case when ul.state is not null then ', ' || ul.state else '' end
                ,case when ul.zip is not null then ', ' || ul.zip else '' end
                ,case when ul.country is not null then ', ' || ul.country else '' end
            ) as full_address
            ,ul.phone as lead_phone

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
                -- when ul.lead_flag is null and tc_client_flag is null and combine.opportunity_revenue > 0 then 'SF lead only'
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
            ,ul.lead_created_date
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

        from
            user_lead ul

            -- sf lead, contact, opportunity
            left join all_contact cont on ul.lead_id = cont.converted_lead_c
            left join distinct_opportunity opp on cont.contact_id = opp.contact_id

            -- TC user
            left join src_tc_user u on u.user_id = ul.user_id
            left join client c on u.user_id = c.user_id
            left join src_tc_line_item li on li.user_id = u.user_id
            left join hs_agent hagent on u.user_id = hagent.transactly_id
            left join src_tc_user_subscription sub on u.user_id = sub.user_id
            left join fifth_order fifth on u.user_id = fifth.user_id
            left join src_hs_owners orig_agent on hagent.original_sales_rep = cast(orig_agent.ownerid as varchar)
            left join subscrip_tier st on u.user_id = st.user_id
            left join role_combine rc on ul.user_id = rc.user_id

            -- orders
            left join first_order_placed fp on u.user_id = fp.user_id
            left join last_order_placed loc on u.user_id = loc.user_id
            left join first_order_closed c1 on u.user_id = c1.user_id
            left join second_order_closed c2 on u.user_id = c2.user_id
            left join third_order_closed c3 on u.user_id = c3.user_id
            left join fourth_order_closed c4 on u.user_id = c4.user_id
            left join fifth_order_closed c5 on u.user_id = c5.user_id

            --states
            left join states on ul.lead_id = states.lead_id


        group by
            u.user_id
            ,ul.lead_id
            ,nvl(replace(u.first_name, '"', ''), ul.first_name)
            ,nvl(replace(u.last_name, '"', ''), ul.last_name)
            ,nvl(replace(u.fullname, '"', ''), ul.fullname)
            ,nvl(replace(u.email, '"', ''), ul.email)
            ,u.brokerage
            ,st.tier
            ,hagent.lead_status
            ,concat(cont.first_name, ' ', cont.last_name)
            ,ul.lead_source
            ,cont.contact_id
            ,u.stripe_account_id
            ,rc.role_super_admin
            ,rc.role_user
            ,rc.role_staff
            ,rc.role_beta_agent
            ,rc.role_agent
            ,rc.role_unlicensed
            ,rc.role_lender
            ,rc.role_partner
            ,rc.role_trans_coordinator
            ,ul.agent_name
            ,ul.agent_email
            ,hagent.address
            ,hagent.address2
            ,ul.street
            ,ul.city
            ,states.state
            ,ul.zip
            ,ul.country
            ,full_address
            ,ul.phone
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
            ,ul.lead_created_date
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
    )

    ,final as(
        select
            working.seq_dim_user.nextval as user_pk
            ,user_id
            ,lead_id
            ,first_name
            ,last_name
            ,fullname
            ,email
            ,brokerage
            ,subscription_level
            ,lead_status as transaction_coordinator_status
            ,contact_owner
            ,contact_id
            ,stripe_account_id

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
            ,agent_name
            ,agent_email
            ,address
            ,address2
            ,original_sales_rep_name

            -- lead address
            ,lead_street
            ,lead_city
            ,lead_state
            ,lead_zip
            ,lead_country
            ,full_address

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
            ,lead_created_date
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

        from final_logic

        group by
            user_pk
            ,user_id
            ,lead_id
            ,first_name
            ,last_name
            ,fullname
            ,email
            ,brokerage
            ,subscription_level
            ,lead_status
            ,contact_owner
            ,contact_id
            ,stripe_account_id
            ,role_super_admin_flag
            ,role_user_flag
            ,role_staff_flag
            ,role_beta_agent_flag
            ,role_agent_flag
            ,role_unlicensed_flag
            ,role_lender_flag
            ,role_partner_flag
            ,role_trans_coordinator_flag
            ,agent_name
            ,agent_email
            ,address
            ,address2
            ,lead_street
            ,lead_city
            ,lead_state
            ,lead_zip
            ,lead_country
            ,full_address
            ,lead_phone
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
            ,lead_created_date
            ,start_date
            ,days_between_start_date_and_first_order_date
            ,tier_3
            ,last_order_due


        union select
            0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null
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