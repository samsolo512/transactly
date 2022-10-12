-- fact_revenue
-- 1 row/user/day

with
    fact_opportunity as(
        select *
        from {{ ref('fact_opportunity')}}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user')}}
    )

    ,fact_line_item as(
        select *
        from {{ ref('fact_line_item')}}
    )

    ,dim_line_item as(
        select *
        from {{ ref('dim_line_item')}}
    )

    -- opportunity revenue
    ,opp as(
        select
            u.user_pk
            -- u.lead_id
            -- null as user_id
            -- ,u.fullname
            ,fact.close_date as date
            -- ,u.lead_flag
            -- ,u.tc_client_flag
            ,sum(fact.revenue) as opportunity_revenue
            ,0 as transactly_revenue

        from
            fact_opportunity fact
            join dim_user u on fact.user_pk = u.user_pk
--             join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk

        where
            revenue_connection_flag = 1

        group by u.user_pk, fact.close_date
    )

    -- transactly revenue
    ,TC as(
        select
            u.user_pk
            -- null as lead_id
            -- ,u.user_id
            -- ,u.fullname
            ,line.due_date as date
            -- ,u.lead_flag
            -- ,u.tc_client_flag
            ,0 as opportunity_revenue
            ,sum(line.total_fees) as transactly_revenue

        from
            fact_line_item fact
            join dim_user u on fact.user_pk = u.user_pk
            join dim_line_item line on fact.line_item_pk = line.line_item_pk

        group by u.user_pk, line.due_date
    )

    ,combine as(
        select
            user_pk
            ,date
            ,sum(opportunity_revenue) as opportunity_revenue
            ,sum(transactly_revenue) as transactly_revenue
        from opp
        group by user_pk, date

        union
        select
            user_pk
            ,date
            ,opportunity_revenue
            ,transactly_revenue
        from TC
        group by user_pk, date, opportunity_revenue, transactly_revenue
    )

    ,final as(
        select
            u.user_pk

            ,combine.date
            ,u.lead_flag
            ,u.tc_client_flag
            ,case
                when u.lead_flag = 0 and u.tc_client_flag = 1 then 'TC client only'
                when u.lead_flag = 1 and u.tc_client_flag = 0 then 'SF lead only'
                when u.lead_flag = 1 and u.tc_client_flag = 1 then 'TC client and SF lead'
                when u.lead_flag = 0 and u.tc_client_flag = 0  then 'TC client only'
                when u.lead_flag = 0 and u.tc_client_flag = 0  and user_id is not null and user_id <> 0 then 'TC client only'
                when u.lead_flag is null and u.tc_client_flag is null and combine.opportunity_revenue > 0 then 'SF lead only'
                else null
                end as client_type
            ,combine.opportunity_revenue
            ,combine.transactly_revenue
            ,combine.opportunity_revenue + combine.transactly_revenue as total_revenue

        from
            combine
            join dim_user u on combine.user_pk = u.user_pk
    )

select * from final

