-- dim_vendor_payout

with
    src_sf_vendor_payout_c as(
        select *
        from {{ ref('src_sf_vendor_payout_c') }}
    )

    ,final as(
        select
            working.seq_dim_vendor_payout.nextval as vendor_payout_pk
            ,vendor_payout_id
            ,vendor_payout_name

        from 
            src_sf_vendor_payout_c
    )    

select * from final