with src_tc_member as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'member') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    id as member_id
    ,transaction_id
    ,user_id
    ,party_id
    ,role_id
    ,is_active
    ,office_id

from src_tc_member

where
    _fivetran_deleted = 'FALSE'
--     and is_active = 'TRUE'
