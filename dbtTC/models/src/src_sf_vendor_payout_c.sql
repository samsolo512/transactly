with src_sf_vendor_payout_c as(
    select *
    from {{ source('salesforce_salesforce', 'vendor_payout_c') }}
)

select
    p.name
    ,p.amount_c
    ,p.id as vendor_payout_id
    ,p.date_c as payout_date
    ,p.opportunity_c as opportunity_id
from src_sf_vendor_payout_c p
where
    is_deleted = 'FALSE'
