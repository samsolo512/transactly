
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_opportunity')}}"
    ]
) }}

with
    fact_opportunity as(
        select *
        from {{ ref('fact_opportunity') }}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,final as(
        select
            o.opportunity_line_item_name as opportunity_name
            ,u.lead_state as state
            ,u.lead_street as street
            ,o.account_name
            ,o.opportunity_name as opportunity_owner
            ,u.email
            ,fact.close_date
            ,u.agent_name
            ,u.agent_email
            ,o.product_name
            ,o.product_family
            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,o.stage
            ,sum(fact.revenue) as revenue

        from
            fact_opportunity fact
            join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk
            join dim_user u on fact.user_pk = u.user_pk

        group by
            o.opportunity_line_item_name
            ,u.lead_state
            ,u.lead_street
            ,o.account_name
            ,o.opportunity_name
            ,o.opportunity_name
            ,u.email
            ,fact.close_date
            ,u.agent_name
            ,u.agent_email
            ,o.product_name
            ,o.product_family
            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,o.stage
    )

select * from final

