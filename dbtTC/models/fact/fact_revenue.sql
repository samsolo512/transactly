-- fact_revenue
-- 1 row/agent/date

with
    fact_opportunity as(
        select *
        from {{ ref('fact_opportunity')}}
    )

    ,dim_agent as(
        select *
        from {{ ref('dim_agent')}}
    )

    ,fact_line_item as(
        select *
        from {{ ref('fact_line_item')}}
    )

    ,dim_line_item as(
        select *
        from {{ ref('dim_line_item')}}
    )

    ,src_sf_vendor_payout_c as(
        select *
        from {{ ref('src_sf_vendor_payout_c')}}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity')}}
    )

    -- opportunity revenue
    ,opp as(
        select
            a.agent_pk
            ,o.opportunity_id
            ,fact.close_date as date
            ,sum(fact.revenue) as opportunity_revenue
            ,0 as transactly_revenue
            ,0 as payout_revenue

        from
            fact_opportunity fact
            join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk
            left join dim_agent a on fact.agent_pk = a.agent_pk

        where
            revenue_connection_flag = 1

        group by
            a.agent_pk, fact.close_date, o.opportunity_id
    )

    -- transactly revenue
    ,TC as(
        select
            a.agent_pk
            ,line.due_date as date
            ,0 as opportunity_revenue
            ,sum(line.total_fees) as transactly_revenue
            ,0 as payout_revenue

        from
            fact_line_item fact
            join dim_line_item line on fact.line_item_pk = line.line_item_pk
            left join dim_agent a on fact.agent_pk = a.agent_pk

        where
            line.due_date is not null
            and line.description in('Transaction Coordination fee', 'Listing Coordination Fee')
            and lower(status) not in('cancelled', 'withdrawn')

        group by
            a.agent_pk, line.due_date
    )

    -- vendor payout revenue
    ,vendor_revenue as(
        select
            a.agent_pk
            ,opp.opportunity_id
            ,v.vendor_payout_id
            ,v.payout_date as date
            ,0 as opportunity_revenue
            ,0 as transactly_revenue
            ,sum(v.amount_c) as payout_revenue

        from
            fact_opportunity fact
            join dim_opportunity opp on fact.opportunity_pk = opp.opportunity_pk
            left join dim_agent a on fact.agent_pk = a.agent_pk
            left join src_sf_vendor_payout_c v on opp.opportunity_id = v.opportunity_id

        where
            v.amount_c is not null
            
        group by
            a.agent_pk, v.payout_date, v.vendor_payout_id, opp.opportunity_id
    )
   
   ,combine as(
        select
            agent_pk
            ,opportunity_id
            ,null as vendor_payout_id
            ,cast(date as date) as date
            ,sum(opportunity_revenue) as opportunity_revenue
            ,sum(transactly_revenue) as transactly_revenue
            ,sum(payout_revenue) as vendor_payout_amount
        from opp
        group by agent_pk, opportunity_id, date

        union
        select
            agent_pk
            ,null as opportunity_id
            ,null as vendor_payout_id
            ,cast(date as date) as date
            ,sum(opportunity_revenue) as opportunity_revenue
            ,sum(transactly_revenue) as transactly_revenue
            ,sum(payout_revenue) as vendor_payout_amount
        from TC
        group by agent_pk, date

        union
        select
            agent_pk
            ,opportunity_id
            ,vendor_payout_id
            ,cast(date as date) as date
            ,sum(opportunity_revenue) as opportunity_revenue
            ,sum(transactly_revenue) as transactly_revenue
            ,sum(payout_revenue) as vendor_payout_amount
        from vendor_revenue
        group by agent_pk, vendor_payout_id, date, opportunity_id
    )

    ,final as(
        select
            nvl(a.agent_pk, 0) as agent_pk
            ,nvl(opp.opportunity_pk, 0) as opportunity_pk

            ,c.vendor_payout_id
            ,c.date
            ,case
                when c.opportunity_revenue > 0 then 'opportunity revenue'
                when c.transactly_revenue > 0 then 'TC revenue'
                when c.vendor_payout_amount > 0 then 'vendor payout'
                else null
                end as revenue_type
            ,c.opportunity_revenue
            ,c.transactly_revenue
            ,c.vendor_payout_amount
            ,c.opportunity_revenue + c.transactly_revenue + c.vendor_payout_amount as total_revenue

        from
            combine c
            left join dim_agent a on c.agent_pk = a.agent_pk
            left join dim_opportunity opp on c.opportunity_id = opp.opportunity_id
    )

select * from final
