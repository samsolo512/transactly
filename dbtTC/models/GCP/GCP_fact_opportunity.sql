
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

    ,dim_lead as(
        select *
        from {{ ref('dim_lead') }}
    )

    ,dim_product as(
        select *
        from {{ ref('dim_product') }}
    )

    ,final as(
        select
            lead.opportunity_name
            ,lead.state
            ,lead.street
            ,lead.opportunity_partner_name as account_name
            ,lead.owner_name as opportunity_owner
            ,lead.email
            ,lead.opportunity_close_date as close_date
            ,lead.agent_name
            ,lead.agent_email

            ,product.product_name
            ,product.product_family

            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,fact.stage
            ,fact.is_won_flag
            ,sum(fact.revenue) as revenue

        from
            fact_opportunity fact
            join dim_product product on fact.product_pk = product.product_pk
            join dim_lead lead on fact.lead_pk = lead.lead_pk

        group by
            lead.opportunity_name
            ,lead.state
            ,lead.street
            ,lead.opportunity_partner_name
            ,lead.owner_name
            ,lead.email
            ,lead.opportunity_close_date
            ,lead.agent_name
            ,lead.agent_email
            ,product.product_name
            ,product.product_family
            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,fact.stage
            ,fact.is_won_flag
    )

select * from final

