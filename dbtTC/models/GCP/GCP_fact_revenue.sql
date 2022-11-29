{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_revenue')}}"
    ]
) }}

with
    fact_revenue as(
        select *
        from {{ ref('fact_revenue')}}
    )

    ,dim_agent as(
        select *
        from {{ ref('dim_agent')}}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity')}}
    )

    ,final as(
        select
            o.opportunity_id
            ,fact.vendor_payout_id
            ,a.user_id
            ,a.agent_name
            ,a.lead_agent_flag
            ,a.tc_agent_flag
            ,a.tc_created_date
            ,a.lead_created_date
            ,o.account_name
            ,fact.revenue_type
            ,fact.date
            ,fact.opportunity_revenue
            ,fact.transactly_revenue
            ,fact.vendor_payout_amount
            ,fact.total_revenue

        from
            fact_revenue fact
            join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk
            left join dim_agent a on fact.agent_pk = a.agent_pk
    )

select * from final