-- src_hs_objects_deals

with src_hs_objects_deals as(
    select *
    from {{ source('hs', 'objects_deals') }}
)

select distinct
    property_product_families__c as product_families
    ,d.property_processed_date as processed_date
    ,d.property_unparsed_total_vendor_payouts as commission_amount
    ,d.property_dealstage as dealstage
    ,d.property_partner_object_id as partner_object_id
    ,d.property_hs_object_id as object_id
    ,d.property_amount as amount
from 
    src_hs_objects_deals as d