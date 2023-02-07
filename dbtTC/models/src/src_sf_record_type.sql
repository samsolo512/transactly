with src_sf_record_type as(
    select *
    from {{ source('salesforce_salesforce', 'record_type') }}
)

select
    id as record_type_id
    ,name as record_type_name
from 
    src_sf_record_type

