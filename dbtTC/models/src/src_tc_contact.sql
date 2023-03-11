with src_tc_contact as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'contact') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    id as contact_id
    ,role_id
    ,side_id
    ,trim({{ field_clean('email') }}) as email
    ,party_id
    ,trim({{ field_clean('last_name') }}) as last_name
    ,trim({{ field_clean('first_name') }}) as first_name
    ,transaction_id
    ,phone

from
    src_tc_contact c

where
    _fivetran_deleted = 'FALSE'
