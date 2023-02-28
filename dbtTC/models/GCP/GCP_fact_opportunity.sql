
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
            -- opportunity
            o.opportunity_name
            ,o.opportunity_line_item_name
            ,o.opportunity_id
            ,o.stage
            ,o.lease_start_date
            ,o.opportunity_owner_name

            -- account and product
            ,o.account_name
            ,o.product_name
            ,o.product_family
            ,o.vendor

            -- lead
            ,l.street as lead_street
            ,l.state as lead_state
            ,l.phone as lead_phone
            ,l.mobile_phone as lead_mobile_phone
            ,l.email as lead_email
            ,l.owner_name as lead_owner_name
            ,l.agent_name as lead_agent_name
            ,l.agent_email as lead_agent_email
            ,l.lead_week_date
            
            -- contact
            ,o.contact_id
            ,o.contact_full_name
            ,o.contact_phone
            ,o.contact_mobile_phone
            ,o.contact_email
            ,o.contact_owner_name
            ,o.contact_attribution

            -- facts
            ,o.service_start_date
            ,fact.created_date
            ,fact.close_date
            ,fact.days_to_close
            ,fact.days_since_created
            ,fact.last_stage_change_date
            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,sum(fact.revenue) as revenue

        from
            fact_opportunity fact
            join dim_opportunity o on fact.opportunity_pk = o.opportunity_pk
            join dim_lead l on fact.lead_pk = l.lead_pk

        group by
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34
    )

select * from final
