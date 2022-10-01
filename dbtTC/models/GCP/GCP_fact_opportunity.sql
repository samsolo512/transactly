
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

    ,dim_product as(
        select *
        from {{ ref('dim_product') }}
    )

    ,dim_date as(
        select *
        from {{ ref('dim_date') }}
    )

    ,final as(
        select
            opp.opportunity_name
            ,opp.state
            ,opp.street
            ,opp.account_name
            ,opp.opportunity_owner
            ,opp.contact_id
            ,opp.email

            ,dt.date_id as close_date

            ,product.product_name
            ,product.product_family

            ,fact.revenue_connection_flag
            ,fact.unpaid_connection_flag
            ,fact.stage
            ,fact.is_won_flag
            ,sum(fact.revenue) as revenue

        from
            fact_opportunity fact
            join dim_opportunity opp on fact.opportunity_pk = opp.opportunity_pk
            join dim_product product on fact.product_pk = product.product_pk
            join dim_date dt on fact.close_date_pk = dt.date_pk
        -- where
        --     dt.date_id between '8/9/2022' and '8/9/2022'
        --     and opp.account_name = 'Transactly'
        --     and fact.is_won_flag = 1

        group by
            opp.opportunity_name, dt.date_id, product.product_name, product.product_family, opp.state, opp.street, opp.account_name, fact.stage, fact.is_won_flag, revenue_connection_flag, unpaid_connection_flag, opp.opportunity_owner, opp.contact_id, opp.email
    )

select * from final
-- order by close_date, opportunity_name
