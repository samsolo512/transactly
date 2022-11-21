with src_tc_office_user as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'office_user') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    o.id as office_user_id
    ,o.user_id
    ,o.office_id
    ,o.created
from
    src_tc_office_user o
where
    _fivetran_deleted = 'FALSE'
