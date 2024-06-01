-- GCP_fact_vendor_payout

{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_vendor_payout')}}"
    ]
) }}

with
    fact_vendor_payout as(
        select *
        from {{ ref('fact_vendor_payout') }}
    )

    ,dim_vendor_payout as(
        select *
        from {{ ref('dim_vendor_payout') }}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity') }}
    )

    ,final as(
        select
            pay.vendor_payout_name
            ,opp.stage
            ,opp.opportunity_id
            ,opp.opportunity_name
            ,opp.product_name
            ,opp.contact_full_name
            ,opp.opportunity_close_date
            ,fact.vendor_payout_date
            ,fact.vendor_payout_amount
            ,pay.vendor_name
            ,pay.spiff

        from
            fact_vendor_payout fact
            left join dim_vendor_payout pay on fact.vendor_payout_pk = pay.vendor_payout_pk
            join dim_opportunity opp on fact.opportunity_pk = opp.opportunity_pk
    )

select * from final
-- select 1 as "one"
