
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

    ,dim_lead as(
        select *
        from {{ ref('dim_lead') }}
    )

    ,final as(
        select
            o.opportunity_name
            ,o.opportunity_line_item_name
            ,o.opportunity_id
            ,l.state
            ,l.street
            ,o.account_name
            ,l.email as lead_email
            ,o.owner_name
            ,fact.created_date
            ,fact.close_date
            ,fact.days_to_close
            ,fact.days_since_created
            ,fact.last_stage_change_date
            ,l.agent_name
            ,l.agent_email
            ,o.product_name
            ,o.product_family
            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,o.stage
            ,o.email as contact_email
            ,o.contact_id
            ,sum(fact.revenue) as revenue

        from
            fact_opportunity fact
            join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk
            join dim_lead l on fact.lead_pk = l.lead_pk

        group by
            o.opportunity_name
            ,o.opportunity_line_item_name
            ,o.opportunity_id
            ,l.state
            ,l.street
            ,o.account_name
            ,l.email
            ,o.owner_name
            ,fact.created_date
            ,fact.close_date
            ,fact.days_to_close
            ,fact.days_since_created
            ,fact.last_stage_change_date
            ,l.agent_name
            ,l.agent_email
            ,o.product_name
            ,o.product_family
            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,o.stage
            ,o.email
            ,o.contact_id
    )

select * from final
