-- src_hs_objects_contacts

with src_hs_objects_contacts as(
    select *
    from {{ source('hs', 'objects_contacts') }}
)

select distinct
    property_partner_id as partner_id
    -- ,property_original_referral_partner as original_referral_partner_raw
    ,trim(
        replace(  -- remove tabs
            case 
            when lower(property_original_referral_partner) like '%closesimple%' then 'CloseSimple'
            when lower(property_original_referral_partner) like '%first key%' then 'First Key'
            when lower(property_original_referral_partner) like '%realpage%' then 'RealPage'
            when trim(lower(property_original_referral_partner)) like 'wfg%' then 'WFG National'
            when trim(lower(property_original_referral_partner)) like 'worth clark%' then 'Worth Clark Realty'
            else property_original_referral_partner
            end
            ,chr(9)
            ,''
        )
    ) as original_referral_partner
from 
    src_hs_objects_contacts as d