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

    ,final as(
        select
            u.lead_id
            ,u.user_id
            ,u.fullname
            ,u.lead_flag
            ,u.tc_client_flag
            ,fact.date
            ,fact.client_type
            ,fact.opportunity_revenue
            ,fact.transactly_revenue
            ,fact.payout_revenue
            ,total_revenue

        from
            fact_revenue fact
            join dim_user u on fact.user_pk = u.user_pk
    )

select * from final