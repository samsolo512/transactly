with src_tc_contact as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'contact') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    id as contact_id
    ,role_id
    ,side_id
    ,email
    ,party_id
    ,trim(last_name) as last_name
    ,trim({{ field_clean('first_name') }}) as first_name
    ,transaction_id
    ,phone

from
    src_tc_contact c

where
    _fivetran_deleted = 'FALSE'
