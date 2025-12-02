-- src_hs_objects_contacts

with src_hs_objects_contacts as(
    select *
    from {{ source('hs', 'objects_contacts') }}
)

select distinct
    property_partner_id as partner_id
    ,d.property_original_referral_partner as original_referral_partner
from 
    src_hs_objects_contacts as d