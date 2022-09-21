with src_tc_contact as(
    select *
    from {{ source('tc', 'contact') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    id as contact_id
    ,role_id
    ,side_id
    ,email
    ,party_id
    ,last_name
    ,first_name
    ,transaction_id

from
    src_tc_contact c

where
    _fivetran_deleted = 'FALSE'
