with src_tc_address as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'address') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    a.id as address_id
    ,trim({{ field_clean('a.address_line_1') }}) as street
    ,trim({{ field_clean('a.city') }}) as city
    ,a.state
    ,a.zip

from src_tc_address a

where _fivetran_deleted = 'FALSE'
