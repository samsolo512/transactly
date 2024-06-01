with
    src_sf_vendor_payout_c as(
        select *
        from {{ ref('src_sf_vendor_payout_c') }}
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
            pay.vendor_payout_pk
            ,o.opportunity_pk
            ,v.vendor_payout_date
            ,v.vendor_payout_amount

        from
            src_sf_vendor_payout_c v
            join dim_vendor_payout pay on v.vendor_payout_id = pay.vendor_payout_id
            join dim_opportunity o on v.opportunity_id = o.opportunity_id
    )

select * from final
-- select 1 as "one"
