with src_sf_vendor_payout_c as(
    select *
    from {{ source('salesforce_salesforce', 'vendor_payout_c') }}
)

select
    p.name as vendor_payout_name
    ,p.amount_c as vendor_payout_amount
    ,p.id as vendor_payout_id
    ,p.date_c as vendor_payout_date
    ,p.opportunity_c as opportunity_id
from 
    src_sf_vendor_payout_c p
where
    is_deleted = 'FALSE'
