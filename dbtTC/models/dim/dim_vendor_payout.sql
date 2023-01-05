-- dim_vendor_payout

with
    src_sf_vendor_payout_c as(
        select *
        from {{ ref('src_sf_vendor_payout_c') }}
    )

    ,final as(
        select
            working.seq_dim_vendor_payout.nextval as vendor_payout_pk
            ,v.vendor_payout_id
            ,v.vendor_payout_name
            ,v.spiff
            ,a.account_name as vendor_name

        from 
            src_sf_vendor_payout_c v
            left join src_sf_account a on v.vendor_c = a.account_id
    )    

select * from final