with src_sf_vendor_payout_c as(
    select *
    from {{ source('sf', 'vendor_payout_c') }}
)

select
    p.name
    ,p.amount_c
from src_sf_vendor_payout_c p
where
    is_deleted = 'FALSE'
