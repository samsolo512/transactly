-- src_hs_object_properties

with src_HS_object_properties as(
    select
        objectid
        ,name
        ,value
        ,objecttypeid
    from {{ source('hs', 'object_properties') }}
)

select
    objectid
    ,objecttypeid
    ,name
    ,trim({{ field_clean('value') }}) as value
from 
    src_HS_object_properties
