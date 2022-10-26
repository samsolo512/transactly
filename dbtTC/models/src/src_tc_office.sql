with src_tc_office as(
    select *
    from {{ source('tc', 'office') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    o.id as office_id
    ,o.name as office_name
    ,o.parent_office_id
    ,o.referral_amount
    ,o.agreement_type
from
    src_tc_office o
where
    _fivetran_deleted = 'FALSE'
