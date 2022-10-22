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

    ,dim_user as(
        select *
        from {{ ref('dim_user')}}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity')}}
    )

    ,final as(
        select
            o.opportunity_id
            ,fact.vendor_payout_id
            ,u.lead_id
            ,u.user_id
            ,u.fullname
            ,u.lead_flag
            ,u.tc_client_flag
            ,u.client_type
            ,u.agent_name
            ,o.account_name
            ,fact.revenue_type
            ,fact.date
            ,fact.opportunity_revenue
            ,fact.transactly_revenue
            ,fact.vendor_payout_amount
            ,fact.total_revenue

        from
            fact_revenue fact
            join dim_user u on fact.user_pk = u.user_pk
            join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk
    )

select * from final